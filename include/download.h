#ifndef _H_RR_DOWNLOAD_
#define _H_RR_DOWNLOAD_

#include <stdbool.h>
#include <stdio.h>

typedef struct RRDownload RRDownload;

#define RR_DOWNLOAD_ETAG_SIZE          1024
#define RR_DOWNLOAD_LAST_MODIFIED_SIZE 128

typedef enum RRDownloadResult
{
  RR_DOWNLOAD_RESULT_ERROR,
  RR_DOWNLOAD_RESULT_DOWNLOADED,
  RR_DOWNLOAD_RESULT_NOT_MODIFIED
}
RRDownloadResult;

typedef struct RRDownloadMetadata
{
  char etag        [RR_DOWNLOAD_ETAG_SIZE];
  char lastModified[RR_DOWNLOAD_LAST_MODIFIED_SIZE];
}
RRDownloadMetadata;

bool rr_download_init(RRDownload **ph);
void rr_download_deinit(RRDownload **ph);

void rr_download_set_auth(RRDownload *h, const char *user, const char *pass);
void rr_download_clear_auth(RRDownload *h);

bool rr_download_to_file(RRDownload *h, const char *url, const char *dstFile);
bool rr_download_to_tmpfile(RRDownload *h, const char *url, FILE **out);
RRDownloadResult rr_download_to_tmpfile_conditional(
  RRDownload *h, const char *url, const RRDownloadMetadata *validators,
  FILE **out, RRDownloadMetadata *response);

#endif
