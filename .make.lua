-- agentio's build, as recipes. This replaced the Makefile; there is no other.
--
--   make            the recipes, with what each of them says it does
--   make build      the library
--   make test       the suite
--   make verify     the whole local gate
--
-- At an oslo prompt in this directory `make` is enough; everywhere else it is `oslo make`.
-- The dev shell's toolchain comes from `.env.lua`'s `nix_develop()`, so recipes call `cargo`
-- directly rather than wrapping every command in `nix develop -c`.

local make = oslo.make

local function need(tool, why)
  assert(oslo.run{ "sh", "-c", "command -v " .. tool, capture = true }.ok, why)
end

-- name = ... from Cargo.toml — the one place every tool reads it from.
local function project_name()
  local content = oslo.fs.read("Cargo.toml") or ""
  local name = content:match('\nname%s*=%s*"([^"]+)"') or content:match('^name%s*=%s*"([^"]+)"')
  assert(name, "Cargo.toml package name not found or invalid")
  return name
end

local NAME = project_name()
local TOP_DIR = oslo.sys.pwd()
local AUDIT_DB = TOP_DIR .. "/target/advisory-db"
local AUDIT_IGNORES = { "--ignore", "RUSTSEC-2023-0071" }

make.recipe{ name = "build", desc = "the library",
             run = function() sh.cargo("build", "--lib") end }
make.alias("b", "build")

make.recipe{ name = "compile", desc = "clean, then build", deps = { "clean", "build" } }
make.alias("c", "compile")

make.recipe{
  name = "run",
  desc = "run an example",
  params = {
    { "--example", desc = "which example to run", default = "01_single_machine" },
    { "--args", desc = "arguments passed through to the example" },
  },
  run = function(a)
    sh.sh("-c", ("cargo run --example %s -- %s"):format(a.example or "01_single_machine", a.args or ""))
  end,
}
make.alias("r", "run")

make.recipe{
  name = "camera-publisher",
  desc = "publish RGB frames from --device",
  params = {
    { "--device", desc = "camera device", default = "/dev/video0" },
    { "--args", desc = "extra arguments" },
  },
  run = function(a)
    sh.sh("-c", ("cargo run --example 10_camera_publisher -- --device %q %s")
      :format(a.device or "/dev/video0", a.args or ""))
  end,
}

make.recipe{
  name = "camera-subscriber",
  desc = "subscribe using --server=<did:key>",
  params = {
    { "--server", desc = "endpoint ID or did:key (required)" },
    { "--args", desc = "extra arguments" },
  },
  run = function(a)
    assert(a.server and a.server ~= "", "--server is required (Endpoint ID or did:key)")
    sh.sh("-c", ("cargo run --example 11_camera_subscriber -- %q %s"):format(a.server, a.args or ""))
  end,
}

make.recipe{ name = "test", desc = "run all tests",
             run = function() sh.cargo("test", "--all-targets", "--", "--test-threads=1") end }
make.alias("t", "test")

make.recipe{ name = "integration", desc = "run integration tests",
             run = function() sh.cargo("test", "--tests", "--", "--test-threads=1") end }

make.recipe{ name = "agent-test", desc = "run local and forced-QUIC Agent tests",
             run = function() sh.cargo("test", "--test", "agent_local", "--", "--test-threads=1") end }

make.recipe{ name = "directory-test", desc = "run reconciliation and lease tests",
             run = function() sh.cargo("test", "--test", "directory_reconciliation", "--", "--test-threads=1") end }

make.recipe{ name = "remote-test", desc = "run the forced-QUIC referral test",
             run = function() sh.cargo("test", "--test", "referral_remote", "--", "--test-threads=1") end }

make.recipe{ name = "examples-smoke", desc = "run the bounded exchange example",
             run = function() sh.cargo("run", "--example", "05_all_exchanges") end }

make.recipe{ name = "audit", desc = "scan dependencies for security advisories",
             run = function() sh.cargo("audit", "--db", AUDIT_DB, table.unpack(AUDIT_IGNORES)) end }

make.recipe{ name = "check", desc = "cargo check on all targets",
             run = function() sh.cargo("check", "--all-targets") end }

make.recipe{ name = "check-all", desc = "cargo check on all targets/all features",
             run = function() sh.cargo("check", "--all-targets", "--all-features") end }

make.recipe{ name = "bind-py", desc = "generate the Python bindings",
             run = function() sh.maturin("build", "--features", "python") end }

make.recipe{ name = "fmt", desc = "format the workspace",
             run = function() sh.cargo("fmt", "--package", NAME) end }

make.recipe{ name = "fmt-check", desc = "check formatting",
             run = function() sh.cargo("fmt", "--package", NAME, "--", "--check") end }

make.recipe{ name = "lock", desc = "regenerate Cargo.lock",
             run = function() sh.cargo("generate-lockfile") end }

make.recipe{ name = "clippy", desc = "clippy with warnings denied",
             run = function() sh.cargo("clippy", "--all-targets", "--all-features", "--", "-D", "warnings") end }

make.recipe{
  name = "rustdoc",
  desc = "build docs with warnings denied",
  run = function()
    assert(oslo.run{ "env", "RUSTDOCFLAGS=-Dwarnings", "cargo", "doc", "--all-features", "--no-deps" }.ok,
           "rustdoc failed")
  end,
}

make.recipe{ name = "test-all", desc = "cargo test on all targets/all features",
             run = function() sh.cargo("test", "--all-targets", "--all-features", "--", "--test-threads=1") end }

make.recipe{ name = "clean", desc = "remove Cargo build artifacts",
             run = function() sh.cargo("clean") end }

make.recipe{
  name = "verify",
  desc = "the whole local gate",
  deps = { "fmt-check", "check", "test", "check-all", "test-all", "clippy", "rustdoc" },
}

make.recipe{
  name = "release",
  desc = "cut a version: --type patch | minor | major | M.m.p",
  params = { { "--type", desc = "patch | minor | major | M.m.p" } },
  run = function(a)
    need("git-rel", "git-rel is not installed; install it first")
    assert(type(a.type) == "string",
           "which release? make release --type patch|minor|major|M.m.p")
    sh.git("rel", a.type)
  end,
}
