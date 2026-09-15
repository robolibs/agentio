-- agentio's directory environment. Loaded when you `cd` here, unloaded when you leave.

oslo.direnv.nix_develop()

oslo.direnv.path_add("./")
oslo.direnv.path_add("./bin")
oslo.direnv.path_add("./zig-out/bin")
oslo.direnv.path_add("./target/debug")
oslo.direnv.path_add("./target/release")
oslo.direnv.path_add("./build")
oslo.direnv.path_add(os.getenv("HOME") .. "/go/bin")
oslo.direnv.path_add(os.getenv("HOME") .. "/.nimble/bin")

oslo.env.set("TOP_HEAD", oslo.sys.pwd())

-- A token in the environment is a token in every child process, and nothing in here needs it.
oslo.env.unset("GITHUB_TOKEN")

-- Shared with every other robolibs checkout so the Wayland/NVIDIA detection lives in one place.
oslo.source("/home/bresilla/data/code/robolibs/.display.sh")

oslo.env.set_alias("_b", "make build")
oslo.env.set_alias("_c", "make compile")
oslo.env.set_alias("_r", "make run")
oslo.env.set_alias("_t", "make test")
