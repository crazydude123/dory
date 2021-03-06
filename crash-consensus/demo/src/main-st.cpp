#include <chrono>
#include <dory/crash-consensus.hpp>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <cassert>
#include <condition_variable>

#include "helpers.hpp"
#include "timers.h"
///////////////// Native K-V Implementation: Project - ASMR
#include <cstring>
#include <sstream>
int kvlength = 500000;  // size of the Key-Value Store
int keylength = 64;     // in bytes
int valuelength;
size_t payloadread = 0;  // length of key + value from workload.txt
int timesread = 0;       // number of operations to execute

char aaa[] = "0";  // init k-v store
///////////////// Native K-V Implementation: Project - ASMR
typedef struct keyvalue {
  char* key;
  char* value;
  /* data */
} kv;
int payload_size;
////// Initialize KeyValueStore (kvstore) with null strings
std::vector<kv> kvstore;

///////////////// Native K-V Implementation: Project - ASMR

int hasho(char* h, int size);
void benchmark(int id, std::vector<int> remote_ids, int times, int payload_size,
               int outstanding_req, dory::ThreadBank threadBank);
/// Own hash function ofc
int hasho(char* h, int size) {
  int sum = 0;
  for (int i = 0; i < size; i++) {
    sum = sum * kvlength + h[i];
  }
  // std::cout << "Hasho" << sum << std::endl;
  return (sum ^ 0x7FABF3FF);
}

int main(int argc, char* argv[]) {
  // std::cout << "Am I here?" << std::endl;
  for (int i = 0; i < kvlength; i++) {
    kvstore.push_back({"\0", "\0"});
  }
  if (argc < 4) {
    throw std::runtime_error("Provide the id of the process as argument");
  }

  constexpr int nr_procs = 3;
  constexpr int minimum_id = 1;
  int id = 0;
  switch (argv[1][0]) {
    case '1':
      id = 1;
      break;
    case '2':
      id = 2;
      break;
    case '3':
      id = 3;
      break;
    default:
      throw std::runtime_error("Invalid id");
  }

  payload_size = atoi(argv[2]);
  valuelength = payload_size;
  std::cout << "USING PAYLOAD SIZE = " << payload_size << std::endl;

  int outstanding_req = atoi(argv[3]);
  std::cout << "USING OUTSTANDING_REQ = " << outstanding_req << std::endl;

  // Build the list of remote ids
  std::vector<int> remote_ids;
  for (int i = 0, min_id = minimum_id; i < nr_procs; i++, min_id++) {
    if (min_id == id) {
      continue;
    } else {
      remote_ids.push_back(min_id);
    }
  }

  const int times =
      static_cast<int>(1.5 * 1024) * 1024 * 1024 / (payload_size + 64);
  benchmark(id, remote_ids, times, payload_size, outstanding_req,
            dory::ThreadBank::A);

  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(60));
  }

  return 0;
}

void benchmark(int id, std::vector<int> remote_ids, int times, int payload_size,
               int outstanding_req, dory::ThreadBank threadBank) {
  std::vector<TIMESTAMP_T> latencies_start, replic_latencies_start;
  std::vector<TIMESTAMP_T> latencies_end, replic_latencies_end;

  // start of reading workload from file
  std::ifstream infile("/users/vijaravi/mu-scripts/workload.txt");
  std::vector<std::string> lines;
  std::string line;
  while (std::getline(infile, line)) {
    lines.push_back(line);
  }
  // split each line into words and store all words in a vector
  std::vector<std::vector<std::string>> words;
  std::vector<char*> words11;
  for (int i = 0; i < lines.size(); i++) {
    std::stringstream ss(lines[i]);
    std::string word;
    std::vector<std::string> line_words;
    while (std::getline(ss, word, ' ')) {
      line_words.push_back(word);
    }
    words.push_back(line_words);
  }
  for (int i = 0; i < words.size(); i++) {
    for (int j = 0; j < words[i].size(); j++) {
      words11.push_back(const_cast<char*>(words[i][j].c_str()));
    }
  }
  // if first word in words is "SET", append the next two words and push into
  // new vector; else, append the next word and push into same vector
  std::vector<char*> new_words;
  std::vector<int> flagforread;
  int ii = 0;
  while (ii < words11.size()) {
    if (strcmp(words11[ii], "SET") == 0) {
      char* new_word =
          new char[strlen(words11[ii + 1]) + strlen(words11[ii + 2]) + 1];
      strcpy(new_word, "1");
      strcat(new_word, words11[ii + 1]);
      keylength = strlen(new_word) - 1;
      valuelength = strlen(words11[ii + 2]);
      std::cout << "Length of Key: " << keylength << std::endl;
      std::cout << "Length of Key: " << valuelength << std::endl;
      flagforread.push_back(1);
      // std::cout << strlen(words11[ii + 1]) << strlen(words11[ii + 2]) <<
      // std::endl;
      strcat(new_word, words11[ii + 2]);
      new_words.push_back(new_word);
      ii += 3;
    } else if (strcmp(words11[ii], "GET") == 0) {
      keylength = strlen(words11[ii + 1]);
      std::cout << "Length of Key: " << keylength << std::endl;
      char* new_word = new char[strlen(words11[ii + 1]) + 1];
      strcpy(new_word, "0");
      strcat(new_word, words11[ii + 1]);
      new_words.push_back(new_word);
      flagforread.push_back(0);
      ii += 2;
    } else {
      ii++;
    }
  }
  std::vector<uint8_t*> new_words_uint8;
  for (int i = 0; i < new_words.size(); i++) {
    uint8_t* new_word_uint8 = new uint8_t[strlen(new_words[i]) + 1];
    strcpy(reinterpret_cast<char*>(new_word_uint8), new_words[i]);
    new_words_uint8.push_back((uint8_t*)new_word_uint8);
  }
  timesread = new_words.size();
  payloadread = keylength + valuelength;
  // print all words in new_words_uint8

  // end of reading workload from file

  TIMESTAMP_T chumma;
  // std::cout << "Am I here inside benchmark?" << std::endl;
  TIMESTAMP_T start_latency, end_latency;
  dory::Consensus consensus(id, remote_ids, outstanding_req, threadBank);
  consensus.commitHandler([&payload_size, &end_latency, &latencies_end,
                           &start_latency, &latencies_start, &chumma, &id,
                           &keylength, &valuelength, &timesread, &payloadread,
                           &flagforread, &new_words,
                           &kvstore]([[maybe_unused]] bool leader,
                                     [[maybe_unused]] uint8_t* buf,
                                     [[maybe_unused]] size_t len) {
    GET_TIMESTAMP(start_latency);
    char* keyval = (char*)(buf);
    char keyy[keylength + 1] = "Eight";
    char value[valuelength + 1] = "Eight";
    if (keyval[0] == '0') {
      for (int k = 0; k < keylength; k++) {
        keyy[k] = keyval[k + 1];
      }
      keyy[keylength] = '\0';
      int hashindex = (hasho(keyy, keylength) % kvlength + kvlength) % kvlength;
      if ((strcmp(kvstore[hashindex].key, "\0")) == 0) {
        GET_TIMESTAMP(end_latency);
        std::cout << "Key0: " << keyy << " not found" << std::endl;
        return;
      } else {
        GET_TIMESTAMP(end_latency);
        std::cout << "Key0: " << keyy << " found" << std::endl;
        return;
      }
    }

    for (int k = 0; k < keylength; k++) {
      keyy[k] = keyval[k + 1];
    }
    keyy[keylength] = '\0';
    for (int k = 0; k < valuelength; k++) {
      value[k] = keyval[k + keylength];
    }
    value[valuelength] = '\0';
    // std::cout << "Key1: " << keyy << " Value1: " << value << std::endl;
    GET_TIMESTAMP(chumma);

    int hashindex = (hasho(keyy, keylength) % kvlength + kvlength) % kvlength;
    for (int i = hashindex; i < kvlength + hashindex; i++) {
      int j = i % kvlength;
      if ((strcmp(kvstore[j].key, "\0") == 0) ||
          (strcmp(kvstore[j].key, keyy) == 0)) {
        kvstore[j].key = keyy;
        kvstore[j].value = keyval;
        break;
      }
    }
    GET_TIMESTAMP(end_latency);
  });
  // Wait enough time for the consensus to become ready
  std::cout << "Wait some time" << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(5 + 3 - id));

  if (id == 1) {
    TIMESTAMP_INIT;

    std::vector<uint8_t> payload_buffer(payload_size + 2);
    uint8_t* payload = &payload_buffer[0];

    std::vector<TIMESTAMP_T> timestamps_start(times);
    std::vector<TIMESTAMP_T> timestamps_end(times);
    std::vector<std::pair<int, TIMESTAMP_T>> timestamps_ranges(times);
    TIMESTAMP_T loop_time;

    mkrndstr_ipa(payload_size, payload);
    consensus.propose(payload, payload_size);

    int offset = 2;

    std::vector<std::vector<uint8_t>> payloads(8192);
    for (size_t i = 0; i < payloads.size(); i++) {
      payloads[i].resize(payload_size);
      mkrndstr_ipa(payload_size, &(payloads[i][0]));
    }

    std::cout << "Started" << std::endl;

    TIMESTAMP_T start_meas, end_meas;
    // TIMESTAMP_T start_latency, end_latency;
    GET_TIMESTAMP(start_meas);
    // for (int i = 0; i < times; i++) {
    for (int i = 0; i < timesread; i++) {
      // Encode process doing the proposal
      GET_TIMESTAMP(timestamps_start[i]);
      dory::ProposeError err;
      // std::cout << "Proposing " << i << std::endl;
      // err = consensus.propose(&(payloads[i % 8192][0]), payload_size);
      // std::cout << (char*)new_words_uint8[i] << std::endl;
      err = consensus.propose(&new_words_uint8[(i + 1) % timesread][0],
                              payload_size);
      if (err != dory::ProposeError::NoError) {
        std::cout << "Proposal failed at index " << i << std::endl;
        i -= 1;
        switch (err) {
          case dory::ProposeError::FastPath:
          case dory::ProposeError::FastPathRecyclingTriggered:
          case dory::ProposeError::SlowPathCatchFUO:
          case dory::ProposeError::SlowPathUpdateFollowers:
          case dory::ProposeError::SlowPathCatchProposal:
          case dory::ProposeError::SlowPathUpdateProposal:
          case dory::ProposeError::SlowPathReadRemoteLogs:
          case dory::ProposeError::SlowPathWriteAdoptedValue:
          case dory::ProposeError::SlowPathWriteNewValue:
            std::cout << "Error: in leader mode. Code: "
                      << static_cast<int>(err) << std::endl;
            break;

          case dory::ProposeError::SlowPathLogRecycled:
            std::cout << "Log recycled, waiting a bit..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            break;

          case dory::ProposeError::MutexUnavailable:
          case dory::ProposeError::FollowerMode:
            std::cout << "Error: in follower mode. Potential leader: "
                      << consensus.potentialLeader() << std::endl;
            break;

          default:
            std::cout << "Bug in code. You should only handle errors here"
                      << std::endl;
        }
      }
      GET_TIMESTAMP(loop_time);
      auto [id_posted, id_replicated] = consensus.proposedReplicatedRange();
      (void)id_posted;
      latencies_start.push_back((start_latency));
      latencies_end.push_back((end_latency));
      replic_latencies_start.push_back((timestamps_start[i]));
      replic_latencies_end.push_back((loop_time));
      timestamps_ranges[i] =
          std::make_pair(int(id_replicated - offset), loop_time);
    }
    GET_TIMESTAMP(end_meas);
    std::cout << "Replicated " << times << " commands of size " << payload_size
              << " bytes in " << ELAPSED_NSEC(start_meas, end_meas) << " ns"
              << std::endl;
    std::ofstream dump, dump1, dump2;
    dump.open("dump-st-dd" + std::to_string(payload_size) + "-" +
              std::to_string(outstanding_req) + ".txt");
    int start_range = 0;
    TIMESTAMP_T last_received;
    GET_TIMESTAMP(last_received);
    for (unsigned int i = 0; i < latencies_start.size(); i++) {
      // dump << ELAPSED_NSEC(latencies_start.at(i), latencies_end.at(i)) <<
      // "\n";
      dump << (latencies_start.at(i).tv_nsec +
               latencies_start.at(i).tv_sec * 1000000000UL)
           << " "
           << (latencies_end.at(i).tv_nsec +
               latencies_end.at(i).tv_sec * 1000000000UL)
           << " " << ELAPSED_NSEC(latencies_start.at(i), latencies_end.at(i))
           << "\n";
    }
    dump.close();

    dump1.open("dump-st-" + std::to_string(payload_size) + "-" +
               std::to_string(outstanding_req) + ".txt");

    for (size_t i = 0; i < timestamps_ranges.size(); i++) {
      auto [last_id, timestamp] = timestamps_ranges[i];
      for (int j = start_range; j < last_id; j++) {
        last_received = timestamp;
        dump1 << ELAPSED_NSEC(timestamps_start[j], timestamp) << "\n";
      }

      if (start_range < last_id) {
        start_range = last_id;
      }
    }
    dump1.close();
    dump2.open("dump-st-repli-" + std::to_string(payload_size) + "-" +
               std::to_string(outstanding_req) + ".txt");
    start_range = 0;
    GET_TIMESTAMP(last_received);
    for (unsigned int i = 0; i < replic_latencies_start.size(); i++) {
      dump2 << (replic_latencies_start.at(i).tv_nsec +
                replic_latencies_start.at(i).tv_sec * 1000000000UL)
            << " "
            << (replic_latencies_end.at(i).tv_nsec +
                replic_latencies_end.at(i).tv_sec * 1000000000UL)
            << " "
            << ELAPSED_NSEC(replic_latencies_start.at(i),
                            replic_latencies_end.at(i))
            << "\n";
    }
    dump2.close();

    exit(0);
  }
}