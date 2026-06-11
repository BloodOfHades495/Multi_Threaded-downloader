// multithreadeddownloader.cpp : This file contains the 'main' function. Program execution begins and ends there.


#include "downloader.h"

#include <csignal>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using namespace mtd;
namespace fs = std::filesystem;

namespace {

    std::atomic<bool> g_cancel{ false };

    void signalHandler(int) {
        g_cancel = true;
        std::cerr << "\nCancellation requested. Saving metadata and stopping...\n";
    }

    void usage(const char* exe) {
        std::cout << R"USAGE(High-performance C++20 parallel download manager

Usage:
  )USAGE" << exe << R"USAGE( [options] <url1> [url2 ...]

Core options:
  -o, --output <path>          Output file for one URL, or output directory for multiple URLs
  -j, --workers <n>            Worker threads per download (default: 8)
  --concurrent <n>             Concurrent downloads in queue (default: 1)
  -c, --chunk <size>           Chunk size, e.g. 4M, 16M, 1G (default: 8M)
  --limit <size/s>             Global bandwidth limit per download, e.g. 2M, 500K (default: unlimited)
  --sha256 <hex>               Expected SHA-256 for integrity verification (single URL)
  --mirror <url>               Mirror URL; can be repeated. Used for failover/retry
  --proxy <proxy-url>          Proxy, e.g. http://127.0.0.1:8080 or socks5://127.0.0.1:1080
  --priority <n>               Queue priority; higher starts earlier (default: 0)
  --retries <n>                Retries per chunk (default: 5)
  --metadata <path>            Metadata path for single URL (default: <output>.mtdmeta)
  --stats <path>               Statistics TSV path (default: downloads.stats.tsv)
  --log <path>                 Log file path
  --quiet                      Disable live progress display
  --insecure                   Disable TLS certificate verification
  --interactive                Accept runtime commands: pause, resume, cancel, status
  -h, --help                   Show help

Examples:
  )USAGE" << exe << R"USAGE( -j 16 -c 16M -o ubuntu.iso https://example.com/ubuntu.iso
  )USAGE" << exe << R"USAGE( --limit 5M --sha256 <hash> -o big.bin https://example.com/big.bin
  )USAGE" << exe << R"USAGE( --concurrent 2 -o downloads/ https://a/file1.zip https://b/file2.iso

Build:
  Linux/macOS: g++ -std=c++20 -O2 downloader.cpp multithreadeddownloader.cpp -lcurl -lssl -lcrypto -pthread -o mtd
  Windows/MSYS2: g++ -std=c++20 -O2 downloader.cpp multithreadeddownloader.cpp -lcurl -lssl -lcrypto -lws2_32 -lcrypt32 -pthread -o mtd.exe
)USAGE";
    }

    std::string basenameFromUrlLocal(const std::string& url) {
        auto q = url.find('?');
        std::string clean = q == std::string::npos ? url : url.substr(0, q);
        auto slash = clean.find_last_of('/');
        std::string name = slash == std::string::npos ? clean : clean.substr(slash + 1);
        if (name.empty()) name = "download.bin";
        for (char& c : name) if (c == ':' || c == '\\' || c == '/' || c == '*' || c == '?' || c == '"' || c == '<' || c == '>' || c == '|') c = '_';
        return name;
    }

    std::uint64_t parseSize(std::string s) {
        if (s.empty()) return 0;
        if (!s.empty() && s.back() == '/') s.pop_back(); // accept 2M/s
        char suffix = 0;
        if (!std::isdigit(static_cast<unsigned char>(s.back()))) {
            suffix = static_cast<char>(std::toupper(static_cast<unsigned char>(s.back())));
            s.pop_back();
        }
        double value = std::stod(s);
        switch (suffix) {
        case 'K': value *= 1024.0; break;
        case 'M': value *= 1024.0 * 1024.0; break;
        case 'G': value *= 1024.0 * 1024.0 * 1024.0; break;
        case 'T': value *= 1024.0 * 1024.0 * 1024.0 * 1024.0; break;
        default: break;
        }
        return static_cast<std::uint64_t>(value);
    }

    bool isOption(const std::string& s) { return s.rfind("-", 0) == 0; }

    std::string requireValue(int& i, int argc, char** argv, const std::string& opt) {
        if (i + 1 >= argc) throw std::runtime_error("Missing value for " + opt);
        return argv[++i];
    }

} // namespace

int main(int argc, char** argv) {
    std::signal(SIGINT, signalHandler);
#ifdef SIGTERM
    std::signal(SIGTERM, signalHandler);
#endif

    if (argc <= 1) { usage(argv[0]); return 0; }

    try {
        DownloadOptions base;
        std::size_t concurrent = 1;
        bool interactive = false;
        fs::path output;
        fs::path metadata;
        std::vector<std::string> urls;

        for (int i = 1; i < argc; ++i) {
            std::string a = argv[i];
            if (a == "-h" || a == "--help") { usage(argv[0]); return 0; }
            else if (a == "-o" || a == "--output") output = requireValue(i, argc, argv, a);
            else if (a == "-j" || a == "--workers") base.workers = static_cast<std::size_t>(std::stoull(requireValue(i, argc, argv, a)));
            else if (a == "--concurrent") concurrent = static_cast<std::size_t>(std::stoull(requireValue(i, argc, argv, a)));
            else if (a == "-c" || a == "--chunk") base.chunkSize = parseSize(requireValue(i, argc, argv, a));
            else if (a == "--limit") base.bandwidthLimitBytesPerSec = parseSize(requireValue(i, argc, argv, a));
            else if (a == "--sha256") base.expectedSha256 = requireValue(i, argc, argv, a);
            else if (a == "--mirror") base.mirrors.push_back({ requireValue(i, argc, argv, a), static_cast<int>(base.mirrors.size()) });
            else if (a == "--proxy") base.proxy = requireValue(i, argc, argv, a);
            else if (a == "--priority") base.priority = std::stoi(requireValue(i, argc, argv, a));
            else if (a == "--retries") base.maxRetries = std::stoi(requireValue(i, argc, argv, a));
            else if (a == "--metadata") metadata = requireValue(i, argc, argv, a);
            else if (a == "--stats") base.statisticsPath = requireValue(i, argc, argv, a);
            else if (a == "--log") Logger::instance().setFile(requireValue(i, argc, argv, a));
            else if (a == "--quiet") base.quiet = true;
            else if (a == "--insecure") base.insecureTls = true;
            else if (a == "--interactive") interactive = true;
            else if (isOption(a)) throw std::runtime_error("Unknown option: " + a);
            else urls.push_back(a);
        }

        if (urls.empty()) throw std::runtime_error("At least one URL is required");
        if (urls.size() > 1 && !base.expectedSha256.empty()) {
            throw std::runtime_error("--sha256 is supported only for a single URL in this CLI. Run one verified file at a time.");
        }
        if (urls.size() > 1 && !metadata.empty()) {
            throw std::runtime_error("--metadata is supported only for a single URL. For queues, metadata defaults to <output>.mtdmeta.");
        }
        if (base.chunkSize == 0) throw std::runtime_error("Chunk size must be greater than zero");

        DownloadManager manager(concurrent);
        std::vector<std::shared_ptr<DownloadTask>> tasks;
        for (const auto& url : urls) {
            DownloadOptions opt = base;
            opt.url = url;
            if (urls.size() == 1) {
                opt.outputPath = output.empty() ? fs::path(basenameFromUrlLocal(url)) : output;
                opt.metadataPath = metadata;
            }
            else {
                fs::path dir = output.empty() ? fs::path("downloads") : output;
                fs::create_directories(dir);
                opt.outputPath = dir / basenameFromUrlLocal(url);
            }
            tasks.push_back(manager.add(opt));
        }

        manager.start();

        std::thread canceller([&] {
            while (!g_cancel) std::this_thread::sleep_for(std::chrono::milliseconds(100));
            manager.cancelAll();
            });

        std::thread console;
        if (interactive) {
            console = std::thread([&] {
                std::cerr << "Interactive commands: pause | resume | cancel | status | quit\n";
                std::string line;
                while (std::getline(std::cin, line)) {
                    std::istringstream iss(line);
                    std::string cmd; iss >> cmd;
                    if (cmd == "pause" || cmd == "p") manager.pauseAll();
                    else if (cmd == "resume" || cmd == "r") manager.resumeAll();
                    else if (cmd == "cancel" || cmd == "quit" || cmd == "q") { manager.cancelAll(); break; }
                    else if (cmd == "status" || cmd == "s") {
                        for (std::size_t i = 0; i < tasks.size(); ++i) {
                            const auto& t = tasks[i];
                            std::cerr << "#" << i << " " << t->options().outputPath.string()
                                << " " << t->progressPercent() << "% "
                                << humanBytes(t->stats().downloadedBytes.load()) << "/"
                                << humanBytes(t->stats().totalBytes) << "\n";
                        }
                    }
                    else if (!cmd.empty()) {
                        std::cerr << "Unknown command: " << cmd << "\n";
                    }
                }
                });
        }

        manager.wait();
        g_cancel = true;
        if (canceller.joinable()) canceller.join();
        if (console.joinable()) console.detach(); // stdin may be blocking; safe at process exit

        bool ok = true;
        for (const auto& t : tasks) ok = ok && t->status() == DownloadStatus::Completed;
        return ok ? 0 : 2;
    }
    catch (const std::exception& e) {
        MTD_LOG_ERROR(e.what());
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}


    