#include <fstream>
#include <fmt/core.h>
#include "ini.h"
#include "params.hpp"

#define CONFIG_FILE "nplex.ini"

static std::string mode_to_string(std::uint8_t mode)
{
    std::string str;

    str += ((mode & NPLEX_CREATE) ? "c" : "-");
    str += ((mode & NPLEX_READ)   ? "r" : "-");
    str += ((mode & NPLEX_UPDATE) ? "u" : "-");
    str += ((mode & NPLEX_DELETE) ? "d" : "-");

    return str;
}

static std::uint8_t string_to_mode(const std::string_view &str)
{
    static const char *crud = "crud";
    std::uint8_t mode = 0;

    if (str.size() != 4)
        throw std::invalid_argument(fmt::format("Invalid crud ({})", str));

    for (std::size_t i = 0; i < 4; i++)
    {
        char c = str[i];

        if (c != crud[i] && c != '-')
            throw std::invalid_argument(fmt::format("Invalid crud ({})", str));

        switch (c)
        {
            case 'c': mode |= NPLEX_CREATE; break;
            case 'r': mode |= NPLEX_READ;   break;
            case 'u': mode |= NPLEX_UPDATE; break;
            case 'd': mode |= NPLEX_DELETE; break;
            default: break;
        }
    }

    return mode;
}

void nplex::server_params_t::load(const fs::path &path)
{
    UNUSED(path);
    // TODO: use inih library to parse INI file
}

void nplex::server_params_t::save() const
{
    if (datadir.empty())
        throw std::runtime_error("datadir not set");

    if (!fs::exists(datadir))
        throw std::runtime_error(datadir.string() + " not exists");

    if (!fs::is_directory(datadir))
        throw std::runtime_error(datadir.string() + " is not a directory");

    auto filepath = datadir / CONFIG_FILE;

    std::ofstream ofs(filepath, std::ios::trunc);

    ofs << "# nplex configuration file." << std::endl;
    ofs << "# see " PROJECT_URL << std::endl;
    ofs << std::endl;

    ofs << "addr = " << addr.str() << std::endl;
    ofs << "log-level = " << to_string(log_level) << std::endl;
    ofs << "max-connections = " << max_connections << std::endl;
    ofs << "disable-fsync = " << (disable_fsync ? "true" : "false") << std::endl;
    ofs << std::endl;

    ofs << "[defaults]" << std::endl;
    ofs << "active = " << (default_user.can_force ? "true" : "false") << std::endl;
    ofs << "max-connections = " << default_user.max_connections << std::endl;
    ofs << "can-force = " << (default_user.can_force ? "true" : "false") << std::endl;
    ofs << "keepalive-millis = " << default_user.keepalive_millis << std::endl;
    ofs << std::endl;

    for (const auto &user : users)
    {
        if (user.name.empty())
            continue;

        ofs << "[" << user.name << "]" << std::endl;
        ofs << "password = " << user.password << std::endl;
        if (user.max_connections != default_user.max_connections)
            ofs << "max-connections = " << user.max_connections << std::endl;
        if (user.keepalive_millis != default_user.keepalive_millis)
            ofs << "keepalive-millis = " << user.keepalive_millis << std::endl;
        if (user.can_force != default_user.can_force)
            ofs << "can-force = " << (user.can_force ? "true" : "false") << std::endl;
        if (user.active != default_user.active)
            ofs << "active = " << (user.active ? "true" : "false") << std::endl;

        for (const auto &acl : user.permissions)
            if (!acl.pattern.empty())
                ofs << "acl = " << mode_to_string(acl.mode) << ":" << acl.pattern << std::endl;

        ofs << std::endl;
    }

    if (users.empty())
    {
        ofs << "[admin]" << std::endl;
        ofs << "active = true" << std::endl;
        ofs << "password = s3cr3t" << std::endl;
        ofs << "max-connections = 10" << std::endl;
        ofs << "keepalive-millis = 1000" << std::endl;
        ofs << "can-force = true" << std::endl;
        ofs << "acl = crud:**" << std::endl;
        ofs << std::endl;
    }

    ofs.close();
}
