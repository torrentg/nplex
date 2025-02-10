#ifndef MATCH_H

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Wildcard string matching and globbing.
 * 
 * @see https://github.com/Robert-van-Engelen/FastGlobbing
 * 
 * @param[in] text Text to match against pattern.
 * @param[in] glob Glob pattern.
 * @return true if text string matches gitignore-style glob pattern
 *         false otherwise
 */
bool glob_match(const char *text, const char *glob);

#ifdef __cplusplus
}
#endif

#endif
