/*
 * This is a modified version based on adler32.c from gst-ffmpeg based on
 * adler32.c from the zlib library.
 *
 * Copyright (C) 1995 Mark Adler
 *
 * This software is provided 'as-is', without any express or implied
 * warranty.  In no event will the authors be held liable for any damages
 * arising from the use of this software.
 *
 * Permission is granted to anyone to use this software for any purpose,
 * including commercial applications, and to alter it and redistribute it
 * freely, subject to the following restrictions:
 *
 * 1. The origin of this software must not be misrepresented; you must not
 *    claim that you wrote the original software. If you use this software
 *    in a product, an acknowledgment in the product documentation would be
 *    appreciated but is not required.
 * 2. Altered source versions must be plainly marked as such, and must not be
 *    misrepresented as being the original software.
 * 3. This notice may not be removed or altered from any source distribution.
 *
 */

#define ADLER32_BASE 65521L /* largest prime smaller than 65536 */

#define ADLER32_DO1(buf)  {s1 += *buf++; s2 += s1;}
#define ADLER32_DO4(buf)  ADLER32_DO1(buf); ADLER32_DO1(buf); ADLER32_DO1(buf); ADLER32_DO1(buf);
#define ADLER32_DO16(buf) ADLER32_DO4(buf); ADLER32_DO4(buf); ADLER32_DO4(buf); ADLER32_DO4(buf);

static uint32_t adler32(uint32_t adler, const void* ptr, uint32_t len)
{
  const uint8_t* buf = (const uint8_t*)ptr;
  uint32_t s1 = adler & 0xffff;
  uint32_t s2 = (adler >> 16) & 0xffff;
  
  while (len > 0) {
    while(len > 16 && s2 < (1U<<31)) {
      ADLER32_DO16(buf); len-=16;
    }
    ADLER32_DO1(buf); len--;
    s1 %= ADLER32_BASE;
    s2 %= ADLER32_BASE;
  }
  return (s2 << 16) | s1;
}
