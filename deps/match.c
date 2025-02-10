// File:        match.c
// Author:      Robert A. van Engelen, engelen@genivia.com
// Date:        August 5, 2019
// See:         https://github.com/Robert-van-Engelen/FastGlobbing
// License:     The Code Project Open License (CPOL)
//              https://www.codeproject.com/info/cpol10.aspx

/*
 * Changes made by torrentg (21-nov-2024):
 *   - Added project url in comment (field 'See')
 *   - Removed all functions except gitignore_glob_match
 *   - Created match.h
 *   - Removed TILDE support
 *   - Removed PATHSEP windows style
 *   - Enum true|FALSE|ABORT replaced by true|false
 *   - Function gitignore_glob_match() renamed to match()
 *   - Added assertions (assert.h)
 *   - Added '**' support to glob_match()
 */

#include <string.h>
#include <ctype.h>
#include <assert.h>
#include "match.h"

// set to 1 to enable dotglob: *. ?, and [] match a . (dotfile) at the begin or after each /
#define DOTGLOB 1

// set to 1 to enable case-insensitive glob matching
#define NOCASEGLOB 0

#define CASE(c) (NOCASEGLOB ? tolower(c) : (c))

#define PATHSEP '/'

// returns true if text string matches glob pattern with * and ?
bool glob_match(const char *text, const char *glob)
{
  const char *text1_backup = NULL;
  const char *glob1_backup = NULL;
  const char *text2_backup = NULL;
  const char *glob2_backup = NULL;
  int nodot = !DOTGLOB;

  assert(text);
  assert(glob);

  while (*text != '\0')
  {
    switch (*glob)
    {
      case '*':
        // match anything except . after /
        if (nodot && *text == '.')
          break;
        if (*++glob == '*')
        {
          // trailing ** match everything after /
          if (*++glob == '\0')
            return true;
          // ** followed by a / match zero or more directories
          if (*glob != '/')
            return false;
          // new **-loop, discard *-loop
          text1_backup = NULL;
          glob1_backup = NULL;
          text2_backup = text;
          glob2_backup = glob;
          if (*text != '/')
            glob++;
          continue;
        }
        // new star-loop: backup positions in pattern and text
        text1_backup = text;
        glob1_backup = glob;
        continue;
      case '?':
        // match anything except . after /
        if (nodot && *text == '.')
          break;
        // match any character except /
        if (*text == PATHSEP)
          break;
        text++;
        glob++;
        continue;
      case '[':
      {
        int lastchr;
        int matched;
        int reverse;
        // match anything except . after /
        if (nodot && *text == '.')
          break;
        // match any character in [...] except /
        if (*text == PATHSEP)
          break;
        // inverted character class
        reverse = glob[1] == '^' || glob[1] == '!' ? true : false;
        if (reverse)
          glob++;
        // match character class
        matched = false;
        for (lastchr = 256; *++glob != '\0' && *glob != ']'; lastchr = CASE(*glob))
          if (lastchr < 256 && *glob == '-' && glob[1] != ']' && glob[1] != '\0' ?
              CASE(*text) <= CASE(*++glob) && CASE(*text) >= lastchr :
              CASE(*text) == CASE(*glob))
            matched = true;
        if (matched == reverse)
          break;
        text++;
        if (*glob != '\0')
          glob++;
        continue;
      }
      case '\\':
        // literal match \-escaped character
        glob++;
        // FALLTHROUGH
      default:
        // match the current non-NUL character
        if (CASE(*glob) != CASE(*text) && !(*glob == '/' && *text == PATHSEP))
          break;
        // do not match a . with *, ? [] after /
        nodot = !DOTGLOB && *glob == '/';
        text++;
        glob++;
        continue;
    }
    if (glob1_backup != NULL && *text1_backup != PATHSEP)
    {
      // *-loop: backtrack to the last * but do not jump over /
      text = ++text1_backup;
      glob = glob1_backup;
      continue;
    }
    if (glob2_backup != NULL)
    {
      // **-loop: backtrack to the last **
      text = ++text2_backup;
      glob = glob2_backup;
      continue;
    }
    return false;
  }
  // ignore trailing stars
  while (*glob == '*')
    glob++;
  // at end of text means success if nothing else is left to match
  return *glob == '\0' ? true : false;
}
