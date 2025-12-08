#include "zip.h"
#include "log.h"

#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <minizip/unzip.h>
#include <minizip/ioapi.h>

typedef struct ZipFileCtx
{
  FILE *fp;
}
ZipFileCtx;

static voidpf ZCALLBACK zopen64_file(voidpf opaque, const void *filename, int mode)
{
  (void)filename;
  ZipFileCtx *ctx = (ZipFileCtx *)opaque;
  if ((mode & ZLIB_FILEFUNC_MODE_READ) == 0)
    return NULL;
  return ctx->fp;
}

static uLong ZCALLBACK zread_file(voidpf opaque, voidpf stream, void* buf, uLong size)
{
  (void)opaque;
  return (uLong)fread(buf, 1, (size_t)size, (FILE *)stream);
}

static uLong ZCALLBACK zwrite_file(voidpf opaque, voidpf stream, const void* buf, uLong size)
{
  return -1;
}

static ZPOS64_T ZCALLBACK ztell64_file(voidpf opaque, voidpf stream)
{
  (void)opaque;
  return (ZPOS64_T)ftello((FILE *)stream);
}

static long ZCALLBACK zseek64_file(voidpf opaque, voidpf stream, ZPOS64_T offset, int origin)
{
  (void)opaque;
  int whence = (origin == ZLIB_FILEFUNC_SEEK_SET) ? SEEK_SET :
               (origin == ZLIB_FILEFUNC_SEEK_CUR) ? SEEK_CUR : SEEK_END;
  return fseeko((FILE *)stream, (off_t)offset, whence);
}

static int ZCALLBACK zclose_file(voidpf opaque, voidpf stream)
{
  ZipFileCtx *ctx = (ZipFileCtx *)opaque;
  free(ctx);
  return 0;
}

static int ZCALLBACK zerror_file(voidpf opaque, voidpf stream)
{
  (void)opaque;
  return ferror((FILE *)stream) ? 1 : 0;
}

unzFile rr_zip_openFILE(FILE *fp)
{
  if (!fp)
    return NULL;

  if (fseek(fp, 0, SEEK_SET) != 0)
  {
    LOG_ERROR("fseek failed");
    return NULL;
  }

  ZipFileCtx *ctx = calloc(1, sizeof(*ctx));
  if (!ctx)
  {
    LOG_ERROR("out of memory");
    return NULL;
  }

  ctx->fp = fp;

  zlib_filefunc64_def ff =
  {
    .opaque       = ctx,
    .zopen64_file = zopen64_file,
    .zread_file   = zread_file,
    .zwrite_file  = zwrite_file,
    .ztell64_file = ztell64_file,
    .zseek64_file = zseek64_file,
    .zclose_file  = zclose_file,
    .zerror_file  = zerror_file
  };

  unzFile uf = unzOpen2_64("", &ff);
  if (!uf)
  {
    LOG_ERROR("unzOpen2_64 failed");
    free(ctx);
    return NULL;
  }

  return uf;
}
