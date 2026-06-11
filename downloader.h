/*
    MultiThreaded Downloader
    ------------------------

    Features:
    - Parallel chunk downloading
    - Resume support
    - SHA-256 verification
    - Download queue scheduling
    - Bandwidth throttling
    - Mirror failover
    - Interactive pause/resume/cancel

    Author: Arnab Mondal (BloodOfHades/BloodOfHades495)
    Language: C++20
*/


#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace mtd {

    namespace fs = std::filesystem;

    // Logging

    enum class LogLevel { Trace = 0, Debug, Info, Warn, Error, Off };

    class Logger {
    public:
        static Logger& instance();
        void setLevel(LogLevel level);
        void setFile(const fs::path& path);
        void log(LogLevel level, const std::string& msg);

    private:
        Logger() = default;
        std::mutex mutex_;
        LogLevel level_ = LogLevel::Info;
        fs::path file_;
    };

#define MTD_LOG_TRACE(msg) ::mtd::Logger::instance().log(::mtd::LogLevel::Trace, (msg))
#define MTD_LOG_DEBUG(msg) ::mtd::Logger::instance().log(::mtd::LogLevel::Debug, (msg))
#define MTD_LOG_INFO(msg)  ::mtd::Logger::instance().log(::mtd::LogLevel::Info,  (msg))
#define MTD_LOG_WARN(msg)  ::mtd::Logger::instance().log(::mtd::LogLevel::Warn,  (msg))
#define MTD_LOG_ERROR(msg) ::mtd::Logger::instance().log(::mtd::LogLevel::Error, (msg))

    // Thread pool

    class ThreadPool {
    public:
        explicit ThreadPool(std::size_t workers = std::thread::hardware_concurrency());
        ~ThreadPool();

        ThreadPool(const ThreadPool&) = delete;
        ThreadPool& operator=(const ThreadPool&) = delete;

        void resize(std::size_t workers);
        std::size_t size() const;

        template<typename F>
        auto submit(F&& f)
            -> std::future<std::invoke_result_t<F>> {
            using R = decltype(f());
            auto task = std::make_shared<std::packaged_task<R()>>(std::forward<F>(f));
            auto fut = task->get_future();
            {
                std::lock_guard<std::mutex> lk(mutex_);
                tasks_.emplace_back([task] { (*task)(); });
            }
            cv_.notify_one();
            return fut;
        }

    private:
        void workerLoop();
        mutable std::mutex mutex_;
        std::condition_variable cv_;
        std::deque<std::function<void()>> tasks_;
        std::vector<std::thread> workers_;
        bool stopping_ = false;
    };

    //  Download model

    enum class ChunkState { Pending, Downloading, Complete, Failed };
    enum class DownloadStatus { Queued, Running, Paused, Completed, Failed, Cancelled };

    struct MirrorUrl {
        std::string url;
        int priority = 0;
    };

    struct DownloadOptions {
        std::string url;
        std::vector<MirrorUrl> mirrors;
        fs::path outputPath;
        fs::path metadataPath;                // Default: <output>.mtdmeta
        fs::path statisticsPath = "downloads.stats.tsv";
        std::string expectedSha256;           // Optional lowercase/uppercase hex
        std::string proxy;                    // Optional http://host:port or socks5://host:port
        std::string userAgent = "mtd-cpp20/1.0";

        std::size_t workers = 8;
        std::uint64_t chunkSize = 8ull * 1024ull * 1024ull; // Adaptive base size
        std::uint64_t minChunkSize = 1ull * 1024ull * 1024ull;
        std::uint64_t maxChunkSize = 64ull * 1024ull * 1024ull;
        std::uint64_t bandwidthLimitBytesPerSec = 0;        // 0 = unlimited
        int maxRetries = 5;
        int connectTimeoutSeconds = 20;
        int lowSpeedLimitBytes = 1024;
        int lowSpeedTimeSeconds = 30;
        int priority = 0;
        bool insecureTls = false;
        bool quiet = false;
        bool continueOnChecksumFailure = false;
    };

    struct DownloadStats {
        std::uint64_t totalBytes = 0;
        std::atomic<std::uint64_t> downloadedBytes{ 0 };
        std::atomic<std::uint64_t> sessionBytes{ 0 };
        std::atomic<std::uint64_t> failedChunks{ 0 };
        std::atomic<std::uint64_t> completedChunks{ 0 };
    };

    struct ChunkInfo {
        std::size_t id = 0;
        std::uint64_t start = 0;
        std::uint64_t end = 0;       // inclusive
        std::uint64_t downloaded = 0;
        std::uint32_t retries = 0;
        ChunkState state = ChunkState::Pending;
        std::string url;
        fs::path tempPath;

        std::uint64_t size() const { return end >= start ? end - start + 1 : 0; }
    };

    struct HttpProbeResult {
        bool ok = false;
        bool rangeSupported = false;
        std::uint64_t contentLength = 0;
        std::string effectiveUrl;
        std::string etag;
        std::string lastModified;
        long responseCode = 0;
    };

    // Main Tasks

    class DownloadTask : public std::enable_shared_from_this<DownloadTask> {
    public:
        explicit DownloadTask(DownloadOptions options);
        ~DownloadTask();

        DownloadTask(const DownloadTask&) = delete;
        DownloadTask& operator=(const DownloadTask&) = delete;

        void start();
        void wait();
        void pause();
        void resume();
        void cancel();

        DownloadStatus status() const;
        const DownloadOptions& options() const { return options_; }
        const DownloadStats& stats() const { return stats_; }
        double progressPercent() const;

        // Used by libcurl callbacks; public so non-member C callbacks can access them.
        void throttle(std::size_t bytes);
        void waitIfPausedOrCancelled();

    private:
        friend class DownloadManager;

        void run();
        HttpProbeResult probe(const std::string& url);
        void prepareChunks(const HttpProbeResult& probe);
        bool loadMetadata();
        void saveMetadata();
        void removeMetadataQuietly();
        bool downloadChunk(std::size_t index);
        bool mergeChunks();
        bool verifyChecksum();
        void monitorLoop();
        std::string pickUrlForAttempt(const ChunkInfo& chunk) const;
        void appendStatistics(bool success, const std::string& message);

        DownloadOptions options_;
        mutable std::mutex mutex_;
        mutable std::mutex chunkMutex_;
        std::condition_variable pauseCv_;
        std::vector<ChunkInfo> chunks_;
        DownloadStats stats_;
        DownloadStatus status_ = DownloadStatus::Queued;
        std::string etag_;
        std::string lastModified_;
        bool rangeSupported_ = false;

        std::unique_ptr<ThreadPool> pool_;
        std::thread mainThread_;
        std::thread monitorThread_;
        std::atomic<bool> stopMonitor_{ false };
        std::atomic<bool> cancelRequested_{ false };
        std::atomic<bool> pauseRequested_{ false };

        // Global download throttling state.
        std::mutex throttleMutex_;
        std::chrono::steady_clock::time_point throttleStart_{};
        std::uint64_t throttledBytes_ = 0;
    };

    // Queue / Scheduler

    class DownloadManager {
    public:
        explicit DownloadManager(std::size_t concurrentDownloads = 2);
        ~DownloadManager();

        std::shared_ptr<DownloadTask> add(DownloadOptions options);
        void start();
        void wait();
        void pauseAll();
        void resumeAll();
        void cancelAll();

    private:
        struct QueueItem {
            int priority;
            std::uint64_t sequence;
            std::shared_ptr<DownloadTask> task;
            bool operator<(const QueueItem& other) const {
                if (priority != other.priority) return priority < other.priority; // high first
                return sequence > other.sequence; // FIFO for same priority
            }
        };

        void schedulerLoop();

        std::size_t concurrentDownloads_;
        std::priority_queue<QueueItem> queue_;
        std::vector<std::shared_ptr<DownloadTask>> allTasks_;
        std::vector<std::shared_ptr<DownloadTask>> running_;
        std::thread scheduler_;
        std::mutex mutex_;
        std::condition_variable cv_;
        std::atomic<bool> stopping_{ false };
        std::uint64_t sequence_ = 0;
    };

    std::string sha256File(const fs::path& path);
    std::string humanBytes(double bytesPerSecondOrBytes);
    std::string formatDuration(std::uint64_t seconds);

} // namespace mtd