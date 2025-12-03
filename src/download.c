#include "download.h"
#include "log.h"

#include <curl/curl.h>
#include <stdlib.h>
#include <string.h>

struct RRDownload
{
  CURL *ch;
  char errBuf[CURL_ERROR_SIZE];
  char authBuf[128];
};

bool rr_download_init(RRDownload **ph)
{
  if (*ph)
  {
    LOG_ERROR("expected *handle to be NULL");
    return false;
  }

  RRDownload *h;
  h = calloc(1, sizeof(*h));
  if (!h)
  {
    LOG_ERROR("out of memory");
    return false;
  }
 
  h->ch = curl_easy_init();
  if (!h->ch)
  {
    LOG_ERROR("curl_easy_init failed");
    return false;
  }

  curl_easy_setopt(h->ch, CURLOPT_FOLLOWLOCATION, 1L       );
  curl_easy_setopt(h->ch, CURLOPT_FAILONERROR   , 1L       );
  curl_easy_setopt(h->ch, CURLOPT_WRITEFUNCTION , NULL     );
  curl_easy_setopt(h->ch, CURLOPT_ERRORBUFFER   , h->errBuf);
  curl_easy_setopt(h->ch, CURLOPT_CONNECTTIMEOUT, 15L      );
  curl_easy_setopt(h->ch, CURLOPT_TIMEOUT       , 0L       );  

  *ph = h;
  return true;
}

void rr_download_deinit(RRDownload **ph)
{
  if (!*ph)
    return;

  RRDownload *h = *ph;
  curl_easy_cleanup(h->ch);

  free(h);
  *ph = NULL;
}

void rr_download_set_auth(RRDownload *h, const char *user, const char *pass)
{
  snprintf(h->authBuf, sizeof(h->authBuf), "%s:%s", user, pass);
  curl_easy_setopt(h->ch, CURLOPT_USERPWD , h->authBuf);
  curl_easy_setopt(h->ch, CURLOPT_HTTPAUTH, CURLAUTH_ANY);
}

void rr_download_clear_auth(RRDownload *h)
{
  memset(h->authBuf, 0, sizeof(h->authBuf));
  curl_easy_setopt(h->ch, CURLOPT_USERPWD , NULL);
  curl_easy_setopt(h->ch, CURLOPT_HTTPAUTH, CURLAUTH_NONE);
}

static bool rr_download(RRDownload *h, const char *url, FILE *fp)
{
  curl_easy_setopt(h->ch, CURLOPT_URL      , url);
  curl_easy_setopt(h->ch, CURLOPT_WRITEDATA, fp);
  CURLcode cc = curl_easy_perform(h->ch);
  if (cc != CURLE_OK)
  {
    LOG_ERROR("curl_easy_perform: %s",
      h->errBuf[0] ? h->errBuf : curl_easy_strerror(cc));
    return false;
  }

  long http_code = 0;
  curl_easy_getinfo(h->ch, CURLINFO_RESPONSE_CODE, &http_code);
  if (http_code >= 400)
  {
    LOG_ERROR("Unexpected response: %d", http_code);
    return false;
  }

  return true;
}

bool rr_download_to_file(RRDownload *h, const char *url, const char *dstFile)
{
  bool ret = false;

  FILE *fp = fopen(dstFile, "wb");
  if (!fp)
  {
    LOG_ERROR("Failed to open %s for writing", dstFile);
    return false;
  }

  ret = rr_download(h, url, fp);

  fclose(fp);
  return ret;
}

bool rr_download_to_tmpfile(RRDownload *h, const char *url, FILE **out)
{
  FILE *fp = tmpfile();
  if (!fp)
  {
    LOG_ERROR("Failed to open a tempfile");
    return false;
  }

  LOG_INFO("tmpfile fd=%d", fileno(fp));
  if (!rr_download(h, url, fp))
  {
    fclose(fp);
    return false;
  }

  *out = fp;
  return true;
}