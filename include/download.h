#ifndef _H_RR_DOWNLOAD_
#define _H_RR_DOWNLOAD_

#include <stdbool.h>
#include <stdio.h>

typedef struct RRDownload RRDownload;

bool rr_download_init(RRDownload **ph);
void rr_download_deinit(RRDownload **ph);

void rr_download_set_auth(RRDownload *h, const char *user, const char *pass);
void rr_download_clear_auth(RRDownload *h);

bool rr_download_to_file(RRDownload *h, const char *url, const char *dstFile);
bool rr_download_to_tmpfile(RRDownload *h, const char *url, FILE **out);

#endif
