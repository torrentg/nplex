#define DOCTEST_CONFIG_IMPLEMENT
#include <csignal>
#include <iostream>
#include <doctest.h>

static void init_widen()
{
    // we do a first call to _M_widen_init() in order to avoid a helgrind data race warning
    std::ostringstream os{};
    os << "unused" << std::endl;
}

int main(int argc, char *argv[])
{
    std::ios::sync_with_stdio(true);
    init_widen();
    signal(SIGPIPE, SIG_IGN);

    doctest::Context ctx;
    ctx.applyCommandLine(argc, argv);
    return ctx.run();
}
