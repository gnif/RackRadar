#include "download.h"
#include "log.h"

#include <curl/curl.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

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

static bool rr_download_add_validator(struct curl_slist **headers,
  const char *name, const char *value, size_t valueSize)
{
  if (!value || !*value)
    return true;

  const size_t nameLen  = strlen(name);
  const size_t valueLen = strnlen(value, valueSize);
  if (valueLen == valueSize || memchr(value, '\r', valueLen) ||
      memchr(value, '\n', valueLen))
  {
    LOG_ERROR("Invalid %s validator", name);
    return false;
  }

  const size_t headerSize = nameLen + 2 + valueLen + 1;
  char        *header     = malloc(headerSize);
  if (!header)
  {
    LOG_ERROR("out of memory");
    return false;
  }

  memcpy(header, name, nameLen);
  memcpy(header + nameLen, ": ", 2);
  memcpy(header + nameLen + 2, value, valueLen + 1);

  struct curl_slist *next = curl_slist_append(*headers, header);
  free(header);
  if (!next)
  {
    LOG_ERROR("curl_slist_append failed");
    return false;
  }

  *headers = next;
  return true;
}

static bool rr_download_copy_header(const char *buffer, size_t length,
  const char *name, char *dst, size_t dstSize)
{
  if (!dstSize)
    return false;

  const size_t nameLen = strlen(name);
  if (length <= nameLen || buffer[nameLen] != ':' ||
      strncasecmp(buffer, name, nameLen) != 0)
    return false;

  const char *start = buffer + nameLen + 1;
  const char *end   = buffer + length;
  while (start < end && (*start == ' ' || *start == '\t'))
    ++start;

  while (end > start &&
    (end[-1] == '\r' || end[-1] == '\n' || end[-1] == ' ' || end[-1] == '\t'))
    --end;

  size_t copyLen = (size_t)(end - start);
  if (copyLen >= dstSize)
    copyLen = dstSize - 1;

  memcpy(dst, start, copyLen);
  dst[copyLen] = '\0';
  return true;
}

static size_t rr_download_header(char *buffer, size_t size, size_t count,
  void *udata)
{
  if (size && count > (size_t)-1 / size)
    return 0;

  const size_t        length   = size * count;
  RRDownloadMetadata *metadata = udata;

  if (length >= 5 && strncasecmp(buffer, "HTTP/", 5) == 0)
    memset(metadata, 0, sizeof(*metadata));

  if (!rr_download_copy_header(buffer, length, "ETag",
      metadata->etag, sizeof(metadata->etag)))
  {
    rr_download_copy_header(buffer, length, "Last-Modified",
      metadata->lastModified, sizeof(metadata->lastModified));
  }

  return length;
}

static void rr_download_clear_request(RRDownload *h)
{
  curl_easy_setopt(h->ch, CURLOPT_HTTPHEADER    , NULL);
  curl_easy_setopt(h->ch, CURLOPT_HEADERFUNCTION, NULL);
  curl_easy_setopt(h->ch, CURLOPT_HEADERDATA    , NULL);
  curl_easy_setopt(h->ch, CURLOPT_WRITEDATA     , NULL);
  curl_easy_setopt(h->ch, CURLOPT_URL           , NULL);
}

static RRDownloadResult rr_download(RRDownload *h, const char *url, FILE *fp,
  const RRDownloadMetadata *validators, RRDownloadMetadata *response)
{
  RRDownloadResult       result  = RR_DOWNLOAD_RESULT_ERROR;
  struct curl_slist     *headers = NULL;
  RRDownloadMetadata    metadata = { 0 };
  CURLcode              cc       = CURLE_OK;
  long                  httpCode = 0;

  h->errBuf[0] = '\0';

  if (validators)
  {
    if (!rr_download_add_validator(&headers, "If-None-Match",
        validators->etag, sizeof(validators->etag)) ||
        !rr_download_add_validator(&headers, "If-Modified-Since",
        validators->lastModified, sizeof(validators->lastModified)))
      goto cleanup;
  }

  cc = curl_easy_setopt(h->ch, CURLOPT_URL, url);
  if (cc != CURLE_OK)
    goto option_error;

  cc = curl_easy_setopt(h->ch, CURLOPT_WRITEDATA, fp);
  if (cc != CURLE_OK)
    goto option_error;

  cc = curl_easy_setopt(h->ch, CURLOPT_HEADERFUNCTION, rr_download_header);
  if (cc != CURLE_OK)
    goto option_error;

  cc = curl_easy_setopt(h->ch, CURLOPT_HEADERDATA, &metadata);
  if (cc != CURLE_OK)
    goto option_error;

  cc = curl_easy_setopt(h->ch, CURLOPT_HTTPHEADER, headers);
  if (cc != CURLE_OK)
    goto option_error;

  cc = curl_easy_perform(h->ch);
  if (cc != CURLE_OK)
  {
    LOG_ERROR("curl_easy_perform: %s",
      h->errBuf[0] ? h->errBuf : curl_easy_strerror(cc));
    goto cleanup;
  }

  cc = curl_easy_getinfo(h->ch, CURLINFO_RESPONSE_CODE, &httpCode);
  if (cc != CURLE_OK)
  {
    LOG_ERROR("curl_easy_getinfo: %s", curl_easy_strerror(cc));
    goto cleanup;
  }

  if (httpCode == 304)
    result = RR_DOWNLOAD_RESULT_NOT_MODIFIED;
  else if (httpCode >= 400)
    LOG_ERROR("Unexpected response: %ld", httpCode);
  else
    result = RR_DOWNLOAD_RESULT_DOWNLOADED;

  if (response)
    *response = metadata;

  goto cleanup;

option_error:
  LOG_ERROR("curl_easy_setopt: %s", curl_easy_strerror(cc));

cleanup:
  rr_download_clear_request(h);
  curl_slist_free_all(headers);
  return result;
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

  ret = rr_download(h, url, fp, NULL, NULL) ==
    RR_DOWNLOAD_RESULT_DOWNLOADED;

  fclose(fp);
  return ret;
}

bool rr_download_to_tmpfile(RRDownload *h, const char *url, FILE **out)
{
  return rr_download_to_tmpfile_conditional(h, url, NULL, out, NULL) ==
    RR_DOWNLOAD_RESULT_DOWNLOADED;
}

RRDownloadResult rr_download_to_tmpfile_conditional(
  RRDownload *h, const char *url, const RRDownloadMetadata *validators,
  FILE **out, RRDownloadMetadata *response)
{
  RRDownloadMetadata savedValidators;
  if (validators && validators == response)
  {
    savedValidators = *validators;
    validators      = &savedValidators;
  }

  *out = NULL;
  if (response)
    memset(response, 0, sizeof(*response));

  FILE *fp = tmpfile();
  if (!fp)
  {
    LOG_ERROR("Failed to open a tempfile");
    return RR_DOWNLOAD_RESULT_ERROR;
  }

  LOG_INFO("tmpfile fd=%d", fileno(fp));
  const RRDownloadResult result =
    rr_download(h, url, fp, validators, response);
  if (result != RR_DOWNLOAD_RESULT_DOWNLOADED)
  {
    fclose(fp);
    return result;
  }

  *out = fp;
  return result;
}
