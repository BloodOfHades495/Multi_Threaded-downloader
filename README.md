# MultiThreaded Downloader

A high-performance multithreaded download manager written in **C++20** that supports parallel chunk downloading, automatic resume, checksum verification, bandwidth throttling, mirror failover, and download scheduling.

The project was developed to explore advanced systems programming concepts including multithreading, networking, synchronization, file I/O, task scheduling, and fault tolerance.

---
## Supported Downloads

The downloader supports downloading any file that is accessible through standard HTTP/HTTPS URLs, including but not limited to:

* ISO images
* ZIP/RAR/7Z archives
* Executable files (.exe)
* PDF documents
* Images and media files
* Software packages
* Large binary files
* Dataset files

The application is file-type agnostic and operates on raw byte streams, allowing it to download any content served by a compatible web server.

## Features

 **Parallel Chunk Downloading**

* Splits files into multiple chunks and downloads them concurrently using a custom thread pool.
* Improves download performance on servers that support HTTP Range Requests.

 **Resume Interrupted Downloads**

* Automatically saves download metadata.
* Continues partially completed downloads after application restart.

 **SHA-256 Integrity Verification**

* Optional checksum validation using OpenSSL.
* Ensures downloaded files are complete and unmodified.

 **Bandwidth Throttling**

* Configurable download speed limits.
* Useful for preventing network congestion.

**Automatic Retry Mechanism**

* Failed chunks are automatically retried.
* Supports configurable retry limits and backoff behavior.

 **Mirror Support**

* Multiple download mirrors can be configured.
* Automatically switches to alternate sources when necessary.

 **Real-Time Progress Dashboard**

* Live terminal interface displaying:

  * Progress percentage
  * Downloaded size
  * Current speed
  * Estimated remaining time
  * Chunk completion status
  * Worker count

 **Download Queue Management**

* Supports multiple downloads.
* Priority-based scheduling system.

 **Interactive Mode**

* Pause downloads
* Resume downloads
* Cancel downloads
* View current status

---

## Technical Highlights

This project demonstrates:

* Modern C++20 programming
* Thread Pool implementation
* Concurrent task scheduling
* Synchronization using mutexes and condition variables
* Atomic operations
* HTTP networking with libcurl
* File system management with std::filesystem
* Metadata persistence and recovery
* SHA-256 hashing using OpenSSL
* Console-based UI design

## Example Dashboard

<img width="743" height="161" alt="image" src="https://github.com/user-attachments/assets/bce349fc-03db-4d21-8fa1-1132c2ff1247" />

During Downloading 

<img width="741" height="191" alt="image" src="https://github.com/user-attachments/assets/1a4293bc-d08d-4419-b3bb-28fad6272d6f" />

After Download

## Build Instructions

### Linux

g++ -std=c++20 -O2 downloader.cpp multithreadeddownloader.cpp \
-lcurl -lssl -lcrypto -pthread -o mtd

### Windows (MSYS2)

g++ -std=c++20 -O2 downloader.cpp multithreadeddownloader.cpp \
-lcurl -lssl -lcrypto -lws2_32 -lcrypt32 -pthread -o mtd.exe


## Usage

Download a file: (ALL BELOW FILES ARE ABSOLUTELY SAFE TO DOWNLOAD)

mtd https://proof.ovh.net/files/100Mb.dat


Specify worker count:

mtd -j 16 https://proof.ovh.net/files/100Mb.dat


Limit bandwidth:

mtd --limit 2M https://example.com/file.iso

Verify SHA-256 checksum:

mtd --sha256 <hash> https://example.com/file.iso

Enable interactive mode:

mtd --interactive https://example.com/file.iso

---

## Technologies Used

* C++20
* STL Threads
* std::filesystem
* libcurl
* OpenSSL
* Visual Studio 2022

---

## Learning Outcomes

During development, the following concepts were explored:

* Multithreading and synchronization
* Concurrent task execution
* Network programming
* Download resumption techniques
* Fault-tolerant system design
* File integrity verification
* Terminal-based user interfaces

---

## Author
   
- Arnab Mondal 

Developed as a systems programming project to demonstrate practical usage of modern C++ and concurrent programming techniques.
