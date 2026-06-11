#include "downloader.h"
#include <algorithm>
#include <chrono>
#include <cctype>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <curl/curl.h>
#include <openssl/evp.h>

#ifdef _WIN32
#include <windows.h>
#endif

#ifdef min
#undef min
#endif

#ifdef max
#undef max
#endif

constexpr const char* RESET = "\033[0m";
constexpr const char* BLUE = "\033[34m";
constexpr const char* GREEN = "\033[32m";
constexpr const char* YELLOW = "\033[33m";
constexpr const char* RED = "\033[31m";
constexpr const char* CYAN = "\033[36m";

namespace mtd {

    using namespace std::chrono_literals;

    namespace {
        static std::mutex g_consoleMutex;


        struct CurlGlobal {
            CurlGlobal() { curl_global_init(CURL_GLOBAL_DEFAULT); }
            ~CurlGlobal() { curl_global_cleanup(); }
        };

        CurlGlobal curlGlobal;

        std::string nowString() {
            auto now = std::chrono::system_clock::now();
            auto t = std::chrono::system_clock::to_time_t(now);
            std::tm tm{};
#ifdef _WIN32
            localtime_s(&tm, &t);
#else
            localtime_r(&t, &tm);
#endif
            std::ostringstream os;
            os << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
            return os.str();
        }

        std::string levelName(LogLevel l) {
            switch (l) {
            case LogLevel::Trace: return "TRACE";
            case LogLevel::Debug: return "DEBUG";
            case LogLevel::Info:  return "INFO";
            case LogLevel::Warn:  return "WARN";
            case LogLevel::Error: return "ERROR";
            default: return "OFF";
            }
        }

        std::string trim(std::string s) {
            auto notSpace = [](unsigned char c) { return !std::isspace(c); };
            s.erase(s.begin(), std::find_if(s.begin(), s.end(), notSpace));
            s.erase(std::find_if(s.rbegin(), s.rend(), notSpace).base(), s.end());
            return s;
        }

        std::string toLower(std::string s) {
            std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return (char)std::tolower(c); });
            return s;
        }

        std::string basenameFromUrl(const std::string& url) {
            auto q = url.find('?');
            std::string clean = q == std::string::npos ? url : url.substr(0, q);
            auto slash = clean.find_last_of('/');
            std::string name = slash == std::string::npos ? clean : clean.substr(slash + 1);
            if (name.empty()) name = "download.bin";
            for (char& c : name) if (c == ':' || c == '\\' || c == '/' || c == '*' || c == '?' || c == '"' || c == '<' || c == '>' || c == '|') c = '_';
            return name;
        }

        std::string makeProgressBar(
            double percent,
            std::size_t width = 40)
        {
            std::size_t filled =
                static_cast<std::size_t>(
                    (percent / 100.0) * width);

            std::string bar;
            for (std::size_t i = 0; i < width; ++i)
            {
                if (i < filled)
                    bar += "#";
                else
                    bar += ".";
            }
            return bar;
        }

        size_t headerCallback(char* buffer, size_t size, size_t nitems, void* userdata) {
            auto* headers = static_cast<std::unordered_map<std::string, std::string>*>(userdata);
            std::string line(buffer, size * nitems);
            auto colon = line.find(':');
            if (colon != std::string::npos) {
                std::string key = toLower(trim(line.substr(0, colon)));
                std::string value = trim(line.substr(colon + 1));
                (*headers)[key] = value;
            }
            return size * nitems;
        }

        struct WriteContext {
            DownloadTask* task{};
            std::ofstream* out{};
            std::atomic<std::uint64_t>* globalDownloaded{};
            std::atomic<std::uint64_t>* sessionBytes{};
            ChunkInfo* chunk{};
            std::mutex* chunkMutex{};
        };

        size_t writeCallback(char* ptr, size_t size, size_t nmemb, void* userdata) {
            auto* ctx = static_cast<WriteContext*>(userdata);
            size_t bytes = size * nmemb;
            try {
                ctx->task->waitIfPausedOrCancelled();
                ctx->out->write(ptr, static_cast<std::streamsize>(bytes));
                if (!*ctx->out) return 0;
                {
                    std::lock_guard<std::mutex> lk(*ctx->chunkMutex);
                    ctx->chunk->downloaded += bytes;
                }
                ctx->globalDownloaded->fetch_add(bytes, std::memory_order_relaxed);
                ctx->sessionBytes->fetch_add(bytes, std::memory_order_relaxed);
                ctx->task->throttle(bytes);
                return bytes;
            }
            catch (...) {
                return 0;
            }
        }

        int xferInfoCallback(void* clientp, curl_off_t, curl_off_t, curl_off_t, curl_off_t) {
            auto* task = static_cast<DownloadTask*>(clientp);
            return task->status() == DownloadStatus::Cancelled ? 1 : 0;
        }

        std::uint64_t fileSizeOrZero(const fs::path& p) {
            std::error_code ec;
            auto s = fs::file_size(p, ec);
            return ec ? 0 : static_cast<std::uint64_t>(s);
        }

    } // namespace

    //  LoGGer 

    Logger& Logger::instance() { static Logger l; return l; }
    void Logger::setLevel(LogLevel level) { std::lock_guard<std::mutex> lk(mutex_); level_ = level; }
    void Logger::setFile(const fs::path& path) { std::lock_guard<std::mutex> lk(mutex_); file_ = path; }

    void Logger::log(LogLevel level, const std::string& msg)
    {
        std::lock_guard<std::mutex> lk(mutex_);

        if (level < level_ || level_ == LogLevel::Off)
            return;

        std::string line =
            "[" + nowString() + "] [" +
            levelName(level) + "] " +
            msg + "\n";

        {
            std::lock_guard<std::mutex> consoleLock(
                g_consoleMutex);

            std::cerr << line;
        }

        if (!file_.empty())
        {
            std::ofstream out(file_, std::ios::app);
            out << line;
        }
    }

    //  Thread POOL 

    ThreadPool::ThreadPool(std::size_t workers) { resize(std::max<std::size_t>(1, workers)); }

    ThreadPool::~ThreadPool() {
        {
            std::lock_guard<std::mutex> lk(mutex_);
            stopping_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) if (t.joinable()) t.join();
    }

    void ThreadPool::resize(std::size_t workers) {
        std::lock_guard<std::mutex> lk(mutex_);
        if (!workers_.empty()) throw std::runtime_error("ThreadPool resize is allowed only before work starts in this implementation");
        workers = std::max<std::size_t>(1, workers);
        for (std::size_t i = 0; i < workers; ++i) workers_.emplace_back([this] { workerLoop(); });
    }

    std::size_t ThreadPool::size() const { std::lock_guard<std::mutex> lk(mutex_); return workers_.size(); }

    void ThreadPool::workerLoop() {
        for (;;) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lk(mutex_);
                cv_.wait(lk, [&] { return stopping_ || !tasks_.empty(); });
                if (stopping_ && tasks_.empty()) return;
                task = std::move(tasks_.front());
                tasks_.pop_front();
            }
            task();
        }
    }

    // Utilities 

    std::string humanBytes(double b) {
        static const char* units[] = { "B", "KiB", "MiB", "GiB", "TiB" };
        int u = 0;
        while (b >= 1024.0 && u < 4) { b /= 1024.0; ++u; }
        std::ostringstream os;
        os << std::fixed << std::setprecision(u == 0 ? 0 : 2) << b << ' ' << units[u];
        return os.str();
    }

    std::string formatDuration(std::uint64_t seconds) {
        std::uint64_t h = seconds / 3600, m = (seconds % 3600) / 60, s = seconds % 60;
        std::ostringstream os;
        if (h) os << h << "h ";
        if (h || m) os << m << "m ";
        os << s << "s";
        return os.str();
    }

    std::string sha256File(const fs::path& path) {
        EVP_MD_CTX* ctx = EVP_MD_CTX_new();
        if (!ctx) throw std::runtime_error("EVP_MD_CTX_new failed");
        if (EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
            EVP_MD_CTX_free(ctx); throw std::runtime_error("EVP_DigestInit_ex failed");
        }
        std::ifstream in(path, std::ios::binary);
        if (!in) { EVP_MD_CTX_free(ctx); throw std::runtime_error("Cannot open file for SHA-256: " + path.string()); }
        std::vector<char> buf(1024 * 1024);
        while (in) {
            in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
            std::streamsize got = in.gcount();
            if (got > 0 && EVP_DigestUpdate(ctx, buf.data(), static_cast<size_t>(got)) != 1) {
                EVP_MD_CTX_free(ctx); throw std::runtime_error("EVP_DigestUpdate failed");
            }
        }
        unsigned char hash[EVP_MAX_MD_SIZE]; unsigned int len = 0;
        if (EVP_DigestFinal_ex(ctx, hash, &len) != 1) {
            EVP_MD_CTX_free(ctx); throw std::runtime_error("EVP_DigestFinal_ex failed");
        }
        EVP_MD_CTX_free(ctx);
        std::ostringstream os;
        for (unsigned int i = 0; i < len; ++i) os << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
        return os.str();
    }

    // download task 

    DownloadTask::DownloadTask(DownloadOptions options) : options_(std::move(options)) {
        if (options_.outputPath.empty()) options_.outputPath = basenameFromUrl(options_.url);
        if (options_.metadataPath.empty()) options_.metadataPath = options_.outputPath.string() + ".mtdmeta";
        if (options_.workers == 0) options_.workers = 1;
    }

    DownloadTask::~DownloadTask() {
        cancel();
        if (mainThread_.joinable()) mainThread_.join();
        stopMonitor_ = true;
        if (monitorThread_.joinable()) monitorThread_.join();
    }

    void DownloadTask::start() {
        std::lock_guard<std::mutex> lk(mutex_);
        if (status_ == DownloadStatus::Running) return;
        status_ = DownloadStatus::Running;
        mainThread_ = std::thread([self = shared_from_this()] { self->run(); });
    }

    void DownloadTask::wait() { if (mainThread_.joinable()) mainThread_.join(); }

    void DownloadTask::pause() { pauseRequested_ = true; { std::lock_guard<std::mutex> lk(mutex_); if (status_ == DownloadStatus::Running) status_ = DownloadStatus::Paused; } saveMetadata(); }

    void DownloadTask::resume() { pauseRequested_ = false; { std::lock_guard<std::mutex> lk(mutex_); if (status_ == DownloadStatus::Paused) status_ = DownloadStatus::Running; } pauseCv_.notify_all(); }

    void DownloadTask::cancel() {
        cancelRequested_ = true;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            if (status_ != DownloadStatus::Completed && status_ != DownloadStatus::Failed) status_ = DownloadStatus::Cancelled;
        }
        pauseCv_.notify_all();
    }

    DownloadStatus DownloadTask::status() const { std::lock_guard<std::mutex> lk(mutex_); return status_; }

    double DownloadTask::progressPercent() const {
        if (stats_.totalBytes == 0) return 0.0;
        return std::min(100.0, 100.0 * (double)stats_.downloadedBytes.load() / (double)stats_.totalBytes);
    }

    HttpProbeResult DownloadTask::probe(const std::string& url) {
        HttpProbeResult r;
        CURL* curl = curl_easy_init();
        if (!curl) return r;

        std::unordered_map<std::string, std::string> headers;

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(
            curl,
            CURLOPT_HTTP_VERSION,
            CURL_HTTP_VERSION_2TLS
        );
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, headerCallback);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, &headers);
        curl_easy_setopt(curl, CURLOPT_USERAGENT, options_.userAgent.c_str());
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, options_.connectTimeoutSeconds);
        curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0L);

        if (!options_.proxy.empty()) curl_easy_setopt(curl, CURLOPT_PROXY, options_.proxy.c_str());

        if (options_.insecureTls)
        {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
        }
        else
        {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
#ifdef CURLSSLOPT_NATIVE_CA
            curl_easy_setopt(curl,
                CURLOPT_SSL_OPTIONS,
                CURLSSLOPT_NATIVE_CA);
#endif
        }

        CURLcode code = curl_easy_perform(curl);

        if (code != CURLE_OK)
        {
            MTD_LOG_ERROR(
                std::string("Probe failed: ")
                + curl_easy_strerror(code)
            );
        }

        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &r.responseCode);

        curl_off_t cl = -1;
        curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &cl);

        if (code != CURLE_OK)
        {
            MTD_LOG_ERROR(
                std::string("Probe failed: ")
                + curl_easy_strerror(code)
            );
        }

        char* eff = nullptr;
        curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &eff);
        r.effectiveUrl = eff ? eff : url;

        curl_easy_cleanup(curl);

        if (code == CURLE_OK && r.responseCode >= 200 && r.responseCode < 400 && cl > 0) {
            r.ok = true;
            r.contentLength = static_cast<std::uint64_t>(cl);
            auto ar = headers.find("accept-ranges");
            r.rangeSupported = ar != headers.end() && toLower(ar->second).find("bytes") != std::string::npos;
            if (auto it = headers.find("etag"); it != headers.end()) r.etag = it->second;
            if (auto it = headers.find("last-modified"); it != headers.end()) r.lastModified = it->second;
        }

        return r;
    }

    void DownloadTask::prepareChunks(const HttpProbeResult& pr) {
        stats_.totalBytes = pr.contentLength;
        etag_ = pr.etag;
        lastModified_ = pr.lastModified;
        rangeSupported_ = pr.rangeSupported;

        fs::create_directories(options_.outputPath.parent_path().empty() ? fs::path(".") : options_.outputPath.parent_path());

        fs::path partDir = options_.outputPath.string() + ".parts";
        fs::create_directories(partDir);

        chunks_.clear();

        std::uint64_t chunkSize = options_.chunkSize;

        //  more workers => smaller chunks, very large files => larger chunks.

        if (pr.contentLength > 8ull * 1024 * 1024 * 1024) chunkSize = std::min(options_.maxChunkSize, chunkSize * 4);
        if (pr.contentLength < 128ull * 1024 * 1024) chunkSize = std::max(options_.minChunkSize, chunkSize / 2);
        if (!pr.rangeSupported) chunkSize = pr.contentLength;

        for (std::uint64_t start = 0, id = 0; start < pr.contentLength; start += chunkSize, ++id) {
            std::uint64_t end = std::min(pr.contentLength - 1, start + chunkSize - 1);
            ChunkInfo c;
            c.id = static_cast<std::size_t>(id); c.start = start; c.end = end; c.url = pr.effectiveUrl;
            c.tempPath = partDir / ("chunk_" + std::to_string(id) + ".part");
            c.downloaded = std::min<std::uint64_t>(fileSizeOrZero(c.tempPath), c.size());
            c.state = c.downloaded == c.size() ? ChunkState::Complete : ChunkState::Pending;
            chunks_.push_back(std::move(c));
        }

        std::uint64_t have = 0, complete = 0;
        for (auto& c : chunks_) { have += c.downloaded; if (c.state == ChunkState::Complete) ++complete; }
        stats_.downloadedBytes = have;
        stats_.completedChunks = complete;

        saveMetadata();
    }

    bool DownloadTask::loadMetadata() {
        if (!fs::exists(options_.metadataPath)) return false;

        std::error_code ec;
        auto metaSize =
            fs::file_size(options_.metadataPath, ec);

        if (ec || metaSize < 32)
        {
            MTD_LOG_WARN(
                "Metadata file appears corrupted"
            );
            return false;
        }

        std::ifstream in(options_.metadataPath);
        if (!in) return false;

        std::map<std::string, std::string> kv;
        std::vector<std::string> chunkLines;
        std::string line;

        while (std::getline(in, line)) {
            if (line.rfind("chunk=", 0) == 0) { chunkLines.push_back(line.substr(6)); continue; }
            auto eq = line.find('=');
            if (eq != std::string::npos) kv[line.substr(0, eq)] = line.substr(eq + 1);
        }

        if (kv["version"] != "2")
        {
            MTD_LOG_WARN(
                "Unsupported metadata version: "
                + kv["version"]
            );
            return false;
        }

        if (kv["url"] != options_.url || kv["output"] != options_.outputPath.string()) return false;

        chunks_.clear();

        stats_.totalBytes = std::stoull(kv["total"]);
        etag_ = kv["etag"];
        lastModified_ = kv["last_modified"];

        if (etag_.empty() &&
            lastModified_.empty())
        {
            MTD_LOG_WARN(
                "Metadata has no ETag or Last-Modified information"
            );
        }

        rangeSupported_ = kv["range"] == "1";

        for (auto& cl : chunkLines) {
            std::stringstream ss(cl); std::string tok; std::vector<std::string> v;
            while (std::getline(ss, tok, '|')) v.push_back(tok);
            if (v.size() < 8) continue;

            ChunkInfo c;
            c.id = std::stoull(v[0]); c.start = std::stoull(v[1]); c.end = std::stoull(v[2]);

            std::uint64_t recordedSize =
                std::stoull(v[3]);

            std::uint64_t actualSize =
                fileSizeOrZero(v[7]);

            if (actualSize > c.size())
            {
                MTD_LOG_WARN(
                    "Chunk "
                    + std::to_string(c.id)
                    + " larger than expected. Resetting.");
                std::error_code ec;
                fs::remove(v[7], ec);
                actualSize = 0;
            }

            if (recordedSize != actualSize)
            {
                MTD_LOG_DEBUG(
                    "Chunk "
                    + std::to_string(c.id)
                    + " metadata mismatch. "
                    "Using actual file size.");
            }

            c.downloaded =
                std::min<std::uint64_t>(
                    actualSize,
                    c.size());

            c.retries = std::stoi(v[4]); c.state = c.downloaded == c.size() ? ChunkState::Complete : ChunkState::Pending;
            c.url = v[6]; c.tempPath = v[7];
            chunks_.push_back(std::move(c));
        }

        std::sort(chunks_.begin(), chunks_.end(), [](const auto& a, const auto& b) { return a.id < b.id; });

        std::uint64_t have = 0, complete = 0;
        for (auto& c : chunks_) { have += c.downloaded; if (c.state == ChunkState::Complete) ++complete; }
        stats_.downloadedBytes = have;
        stats_.completedChunks = complete;

        if (!chunks_.empty())
        {
            MTD_LOG_INFO(
                "Resume metadata loaded: "
                + humanBytes(static_cast<double>(have))
                + " already downloaded"
            );
        }

        return !chunks_.empty();
    }

    void DownloadTask::saveMetadata() {
        std::scoped_lock lk(mutex_, chunkMutex_);

        fs::create_directories(options_.metadataPath.parent_path().empty() ? fs::path(".") : options_.metadataPath.parent_path());

        const fs::path tempMeta =
            options_.metadataPath.string() + ".tmp";

        {
            std::ofstream out(tempMeta, std::ios::trunc);
            out << "version=2\n";
            out << "url=" << options_.url << "\n";
            out << "output=" << options_.outputPath.string() << "\n";
            out << "total=" << stats_.totalBytes << "\n";
            out << "etag=" << etag_ << "\n";
            out << "last_modified=" << lastModified_ << "\n";
            out << "range=" << (rangeSupported_ ? 1 : 0) << "\n";
            for (const auto& c : chunks_)
            {
                out << "chunk="
                    << c.id << '|'
                    << c.start << '|'
                    << c.end << '|'
                    << c.downloaded << '|'
                    << c.retries << '|'
                    << static_cast<int>(c.state) << '|'
                    << c.url << '|'
                    << c.tempPath.string()
                    << '\n';
            }
            out.flush();
        }

        std::error_code ec;
        fs::remove(options_.metadataPath, ec);
        fs::rename(
            tempMeta,
            options_.metadataPath,
            ec
        );

        if (ec)
        {
            MTD_LOG_ERROR(
                "Failed to update metadata: "
                + ec.message()
            );
        }
    }

    void DownloadTask::removeMetadataQuietly() { std::error_code ec; fs::remove(options_.metadataPath, ec); fs::remove_all(options_.outputPath.string() + ".parts", ec); }

    std::string DownloadTask::pickUrlForAttempt(const ChunkInfo& chunk) const {
        if (options_.mirrors.empty()) return chunk.url.empty() ? options_.url : chunk.url;

        std::vector<MirrorUrl> urls = options_.mirrors;
        urls.push_back({ options_.url, 1000000 });
        std::sort(urls.begin(), urls.end(), [](const auto& a, const auto& b) { return a.priority < b.priority; });
        return urls[static_cast<std::size_t>(chunk.retries) % urls.size()].url;
    }

    bool DownloadTask::downloadChunk(std::size_t index) {
        for (;;) {
            if (cancelRequested_) return false;

            ChunkInfo snapshot;
            {
                std::lock_guard<std::mutex> lk(chunkMutex_);
                if (chunks_[index].downloaded >= chunks_[index].size()) { chunks_[index].state = ChunkState::Complete; return true; }

                // without range support, partial resume is impossible; thus restart the single stream safely.
                if (!rangeSupported_ && chunks_[index].downloaded > 0) {
                    stats_.downloadedBytes.fetch_sub(chunks_[index].downloaded, std::memory_order_relaxed);
                    chunks_[index].downloaded = 0;
                    std::ofstream trunc(chunks_[index].tempPath, std::ios::binary | std::ios::trunc);
                }

                if (chunks_[index].retries > options_.maxRetries) { chunks_[index].state = ChunkState::Failed; return false; }
                chunks_[index].state = ChunkState::Downloading;
                snapshot = chunks_[index];
            }

            std::string url = pickUrlForAttempt(snapshot);
            std::uint64_t from = snapshot.start + snapshot.downloaded;
            std::string range = std::to_string(from) + "-" + std::to_string(snapshot.end);

            MTD_LOG_DEBUG("Chunk " + std::to_string(snapshot.id) + " GET " + range + " from " + url);

            std::ofstream out(snapshot.tempPath, std::ios::binary | std::ios::app);
            if (!out) throw std::runtime_error("Cannot open temp chunk file: " + snapshot.tempPath.string());

            CURL* curl = curl_easy_init();
            if (!curl) throw std::runtime_error("curl_easy_init failed");

            WriteContext ctx{ this, &out, &stats_.downloadedBytes, &stats_.sessionBytes, &chunks_[index], &chunkMutex_ };

            curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
            curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
            curl_easy_setopt(
                curl,
                CURLOPT_HTTP_VERSION,
                CURL_HTTP_VERSION_2TLS
            );
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);
            curl_easy_setopt(curl, CURLOPT_USERAGENT, options_.userAgent.c_str());
            curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, options_.connectTimeoutSeconds);
            curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, options_.lowSpeedLimitBytes);
            curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, options_.lowSpeedTimeSeconds);
            curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
            curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, xferInfoCallback);
            curl_easy_setopt(curl, CURLOPT_XFERINFODATA, this);

            if (rangeSupported_) curl_easy_setopt(curl, CURLOPT_RANGE, range.c_str());

            if (!options_.proxy.empty()) curl_easy_setopt(curl, CURLOPT_PROXY, options_.proxy.c_str());

            if (options_.insecureTls)
            {
                curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
                curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
            }
            else
            {
                curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
                curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
#ifdef CURLSSLOPT_NATIVE_CA
                curl_easy_setopt(curl,
                    CURLOPT_SSL_OPTIONS,
                    CURLSSLOPT_NATIVE_CA);
#endif
            }

            CURLcode code = curl_easy_perform(curl);

            long http = 0; curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http);
            curl_easy_cleanup(curl);

            out.close();

            bool okHttp = rangeSupported_ ? (http == 206 || (chunks_.size() == 1 && http == 200)) : (http >= 200 && http < 300);

            bool completedNow = false;
            {
                std::lock_guard<std::mutex> lk(chunkMutex_);
                chunks_[index].downloaded = std::min<std::uint64_t>(fileSizeOrZero(chunks_[index].tempPath), chunks_[index].size());
                if (code == CURLE_OK && okHttp && chunks_[index].downloaded >= chunks_[index].size()) {
                    chunks_[index].state = ChunkState::Complete;
                    stats_.completedChunks.fetch_add(1, std::memory_order_relaxed);
                    completedNow = true;
                }
                else {
                    if (cancelRequested_) return false;
                    chunks_[index].state = ChunkState::Pending;
                    ++chunks_[index].retries;
                    stats_.failedChunks.fetch_add(1, std::memory_order_relaxed);
                    MTD_LOG_WARN("Chunk " + std::to_string(index) + " failed: curl=" + std::to_string(code) + " http=" + std::to_string(http) + " retry=" + std::to_string(chunks_[index].retries));
                }
            }

            saveMetadata();

            if (completedNow) return true;

            std::this_thread::sleep_for(std::chrono::seconds(std::min(30, 1 << std::min(5, (int)snapshot.retries))));
        }
    }

    bool DownloadTask::mergeChunks() {
        fs::path tmpOut = options_.outputPath.string() + ".assembling";
        std::ofstream out(tmpOut, std::ios::binary | std::ios::trunc);
        if (!out) return false;

        std::vector<char> buf(1024 * 1024);
        for (auto& c : chunks_) {
            std::ifstream in(c.tempPath, std::ios::binary);
            if (!in) return false;
            while (in) {
                in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
                if (in.gcount() > 0) out.write(buf.data(), in.gcount());
            }
        }
        out.close();

        std::error_code ec;
        fs::rename(tmpOut, options_.outputPath, ec);
        if (ec) {
            fs::remove(options_.outputPath, ec);
            ec.clear(); fs::rename(tmpOut, options_.outputPath, ec);
        }
        return !ec;
    }

    bool DownloadTask::verifyChecksum() {
        if (options_.expectedSha256.empty()) return true;
        MTD_LOG_INFO("Computing SHA-256 for " + options_.outputPath.string());
        std::string got = sha256File(options_.outputPath);
        std::string exp = toLower(options_.expectedSha256);
        bool ok = toLower(got) == exp;
        if (!ok) MTD_LOG_ERROR("Checksum mismatch. expected=" + exp + " got=" + got);
        else MTD_LOG_INFO("SHA-256 verified: " + got);
        return ok;
    }

    void DownloadTask::monitorLoop()
    {
        auto lastTime =
            std::chrono::steady_clock::now();

        std::uint64_t lastBytes =
            stats_.downloadedBytes.load();

        double avgSpeed = 0.0;
#ifdef _WIN32
        {
            HANDLE hConsole = GetStdHandle(STD_ERROR_HANDLE);

            CONSOLE_SCREEN_BUFFER_INFO csbi;

            if (GetConsoleScreenBufferInfo(hConsole, &csbi))
            {
                DWORD cellCount =
                    csbi.dwSize.X * csbi.dwSize.Y;

                DWORD count;

                FillConsoleOutputCharacter(
                    hConsole,
                    ' ',
                    cellCount,
                    { 0, 0 },
                    &count);

                FillConsoleOutputAttribute(
                    hConsole,
                    csbi.wAttributes,
                    cellCount,
                    { 0, 0 },
                    &count);
            }
        }
#endif

        while (!stopMonitor_)
        {
            std::this_thread::sleep_for(1s);

            if (options_.quiet)
                continue;

            auto now =
                std::chrono::steady_clock::now();

            std::uint64_t currentBytes =
                stats_.downloadedBytes.load();

            double dt =
                std::chrono::duration<double>(
                    now - lastTime).count();

            double instantSpeed =
                dt > 0
                ? (currentBytes - lastBytes) / dt
                : 0;

            avgSpeed =
                avgSpeed * 0.80 +
                instantSpeed * 0.20;

            lastBytes = currentBytes;
            lastTime = now;

            double pct = progressPercent();

            constexpr std::size_t barWidth = 30;

            std::size_t filled =
                std::min(
                    barWidth,
                    static_cast<std::size_t>(
                        pct * barWidth / 100.0));

            std::string bar;

            for (std::size_t i = 0;
                i < barWidth;
                ++i)
            {
                bar +=
                    (i < filled)
                    ? '#'
                    : '.';
            }

            std::uint64_t remaining =
                stats_.totalBytes > currentBytes
                ? stats_.totalBytes - currentBytes
                : 0;

            std::string etaStr = "--";

            if (avgSpeed > 1024)
            {
                etaStr =
                    formatDuration(
                        static_cast<std::uint64_t>(
                            remaining / avgSpeed));
            }

            std::string statusLabel;

            {
                std::lock_guard<std::mutex> lk(
                    mutex_);

                switch (status_)
                {
                case DownloadStatus::Running:
                    statusLabel = "Downloading";
                    break;

                case DownloadStatus::Paused:
                    statusLabel = "Paused";
                    break;

                case DownloadStatus::Completed:
                    statusLabel = "Completed";
                    break;

                case DownloadStatus::Failed:
                    statusLabel = "Failed";
                    break;

                case DownloadStatus::Cancelled:
                    statusLabel = "Cancelled";
                    break;

                default:
                    statusLabel = "Queued";
                }
            }

            {
                std::lock_guard<std::mutex>
                    consoleLock(g_consoleMutex);

#ifdef _WIN32
                HANDLE hConsole = GetStdHandle(STD_ERROR_HANDLE);

                COORD pos;
                pos.X = 0;
                pos.Y = 0;

                SetConsoleCursorPosition(hConsole, pos);
#else
                std::cerr << "\033[H";
#endif


                std::cerr << std::left << std::setw(80)
                    << ("Progress  : [" + bar + "] " +
                        (static_cast<std::ostringstream&&>(
                            std::ostringstream() << std::fixed
                            << std::setprecision(1)
                            << pct << "%")).str())
                    << '\n';

                std::cerr << std::left << std::setw(80)
                    << ("Downloaded: " +
                        humanBytes(currentBytes) +
                        " / " +
                        humanBytes(stats_.totalBytes))
                    << '\n';

                std::cerr << std::left << std::setw(80)
                    << ("Speed     : " +
                        humanBytes(avgSpeed) +
                        "/s")
                    << '\n';

                std::cerr << std::left << std::setw(80)
                    << ("ETA       : " + etaStr)
                    << '\n';

                std::cerr << std::left << std::setw(80)
                    << ("Chunks    : " +
                        std::to_string(stats_.completedChunks.load()) +
                        " / " +
                        std::to_string(chunks_.size()))
                    << '\n';

                std::cerr << std::left << std::setw(80)
                    << ("Workers   : " +
                        std::to_string(options_.workers))
                    << '\n';

                std::cerr << std::left << std::setw(80)
                    << ("Status    : " + statusLabel)
                    << '\n';


                std::cerr.flush();
            }
        }
    }

    void DownloadTask::throttle(std::size_t bytes) {
        if (options_.bandwidthLimitBytesPerSec == 0) return;

        std::unique_lock<std::mutex> lk(throttleMutex_);
        if (throttleStart_ == std::chrono::steady_clock::time_point{}) throttleStart_ = std::chrono::steady_clock::now();

        throttledBytes_ += bytes;
        double expected = static_cast<double>(throttledBytes_) / static_cast<double>(options_.bandwidthLimitBytesPerSec);
        auto target = throttleStart_ + std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::duration<double>(expected));

        lk.unlock();
        auto now = std::chrono::steady_clock::now();
        if (target > now) std::this_thread::sleep_until(target);
    }

    void DownloadTask::waitIfPausedOrCancelled() {
        std::unique_lock<std::mutex> lk(mutex_);
        pauseCv_.wait(lk, [&] { return !pauseRequested_.load() || cancelRequested_.load(); });
        if (cancelRequested_) throw std::runtime_error("cancelled");
    }

    void DownloadTask::appendStatistics(bool success, const std::string& message) {
        if (options_.statisticsPath.empty()) return;
        std::ofstream out(options_.statisticsPath, std::ios::app);
        out << nowString() << '\t' << (success ? "OK" : "FAIL") << '\t' << options_.url << '\t'
            << options_.outputPath.string() << '\t' << stats_.totalBytes << '\t' << stats_.sessionBytes.load()
            << '\t' << message << "\n";
    }

    void DownloadTask::run() {
        bool success = false;
        std::string message;

        try {
            bool resumed = loadMetadata();

            HttpProbeResult pr;
            std::vector<std::string> urls{ options_.url };

            std::sort(
                options_.mirrors.begin(),
                options_.mirrors.end(),
                [](const auto& a, const auto& b)
                {
                    return a.priority < b.priority;
                });

            for (auto& m : options_.mirrors)
                urls.push_back(m.url);

            for (auto& u : urls)
            {
                pr = probe(u);
                if (pr.ok)
                    break;
            }

            if (!pr.ok)
            {
                throw std::runtime_error(
                    "Unable to probe URL or determine content length");
            }

            if (resumed)
            {
                bool mismatch = false;
                if (!etag_.empty() &&
                    !pr.etag.empty() &&
                    etag_ != pr.etag)
                {
                    mismatch = true;
                }
                if (!lastModified_.empty() &&
                    !pr.lastModified.empty() &&
                    lastModified_ != pr.lastModified)
                {
                    mismatch = true;
                }
                if (mismatch)
                {
                    MTD_LOG_WARN(
                        "Remote file changed. "
                        "Discarding resume metadata.");
                    chunks_.clear();
                    removeMetadataQuietly();
                    prepareChunks(pr);
                }
            }
            else
            {
                prepareChunks(pr);
            }

            pool_ = std::make_unique<ThreadPool>(options_.workers);

            stopMonitor_ = false;
            monitorThread_ = std::thread([this] { monitorLoop(); });

            std::vector<std::future<bool>> futures;
            for (std::size_t i = 0; i < chunks_.size(); ++i) {
                if (chunks_[i].state == ChunkState::Complete) continue;
                futures.push_back(pool_->submit([this, i] { return downloadChunk(i); }));
            }

            bool allOk = true;
            for (auto& f : futures) allOk = f.get() && allOk;

            pool_.reset();

            if (cancelRequested_) {
                message = "cancelled";
                std::lock_guard<std::mutex> lk(mutex_);
                status_ = DownloadStatus::Cancelled;
                appendStatistics(false, message);
                return;
            }

            if (!allOk)
                throw std::runtime_error("One or more chunks failed");

            if (!mergeChunks())
                throw std::runtime_error("Failed to merge chunks");

            if (!verifyChecksum() && !options_.continueOnChecksumFailure)
                throw std::runtime_error("SHA-256 verification failed");

            removeMetadataQuietly();

            {
                std::lock_guard<std::mutex> lk(mutex_);
                status_ = DownloadStatus::Completed;
            }

            success = true;
            message = "completed";

            stopMonitor_ = true;

            if (monitorThread_.joinable())
                monitorThread_.join();

            MTD_LOG_INFO(
                "Download completed: "
                + options_.outputPath.string());
        }

        catch (const std::exception& e) {
            stopMonitor_ = true;
            if (monitorThread_.joinable()) monitorThread_.join();
            message = e.what();
            MTD_LOG_ERROR(message);
            {
                std::lock_guard<std::mutex> lk(mutex_);
                if (status_ != DownloadStatus::Cancelled) status_ = DownloadStatus::Failed;
            }
            saveMetadata();
        }
        appendStatistics(success, message);
    }

    // Download Manager

    DownloadManager::DownloadManager(std::size_t concurrentDownloads) : concurrentDownloads_(std::max<std::size_t>(1, concurrentDownloads)) {}

    DownloadManager::~DownloadManager() { cancelAll(); wait(); }

    std::shared_ptr<DownloadTask> DownloadManager::add(DownloadOptions options) {
        auto task = std::make_shared<DownloadTask>(std::move(options));
        std::lock_guard<std::mutex> lk(mutex_);
        allTasks_.push_back(task);
        queue_.push({ task->options().priority, sequence_++, task });
        cv_.notify_all();
        return task;
    }

    void DownloadManager::start() { scheduler_ = std::thread([this] { schedulerLoop(); }); }

    void DownloadManager::wait() { if (scheduler_.joinable()) scheduler_.join(); for (auto& t : allTasks_) t->wait(); }

    void DownloadManager::pauseAll() { for (auto& t : allTasks_) t->pause(); }

    void DownloadManager::resumeAll() { for (auto& t : allTasks_) t->resume(); }

    void DownloadManager::cancelAll() { stopping_ = true; cv_.notify_all(); for (auto& t : allTasks_) t->cancel(); }

    void DownloadManager::schedulerLoop() {
        for (;;) {
            {
                std::lock_guard<std::mutex> lk(mutex_);

                running_.erase(std::remove_if(running_.begin(), running_.end(), [](const auto& t) {
                    auto s = t->status(); return s == DownloadStatus::Completed || s == DownloadStatus::Failed || s == DownloadStatus::Cancelled;
                    }), running_.end());

                while (!queue_.empty() && running_.size() < concurrentDownloads_) {
                    auto item = queue_.top(); queue_.pop();
                    running_.push_back(item.task);
                    item.task->start();
                }

                if ((queue_.empty() && running_.empty()) || stopping_) return;
            }
            std::this_thread::sleep_for(300ms);
        }
    }

} // namespace mtd
