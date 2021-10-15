# This file defines how PyOxidizer application building and packaging is
# performed. See PyOxidizer's documentation at
# https://pyoxidizer.readthedocs.io/en/stable/ for details of this
# configuration file format.

# Configuration files consist of functions which define build "targets."
# This function creates a Python executable and installs it in a destination
# directory.
def make_exe():
    dist = default_python_distribution(python_version="3.8")

    policy = dist.make_python_packaging_policy()
    policy.set_resource_handling_mode("classify")
    policy.resources_location = "in-memory"
    policy.resources_location_fallback = "filesystem-relative:Lib"
    # Test if this works... Windows 10 probably thinks the sqlite3 dll is a virus
    policy.allow_in_memory_shared_library_loading = True

    python_config = dist.make_python_interpreter_config()

    python_config.run_module = "bin.q"

    exe = dist.to_python_executable(
        name="q",

        packaging_policy=policy,

        config=python_config,
    )

    exe.pip_install(["wheel"])

    exe.add_python_resources(exe.pip_install(["-r", "requirements.txt"]))
    exe.add_python_resources(exe.pip_install(["-e", "."]))

    exe.add_python_resources(exe.read_package_root(
        path="./",
        packages=["bin"],
    ))

    return exe

def make_embedded_resources(exe):
    return exe.to_embedded_resources()

def make_install(exe):
    # Create an object that represents our installed application file layout.
    files = FileManifest()

    # Add the generated executable to our install layout in the root directory.
    files.add_python_resource(".", exe)

    return files

def make_msi(exe):
    # See the full docs for more. But this will convert your Python executable
    # into a `WiXMSIBuilder` Starlark type, which will be converted to a Windows
    # .msi installer when it is built.
    builder = exe.to_wix_msi_builder(
        # Simple identifier of your app.
        "q",
        # The name of your application.
        "Q - Text as Data",
        # The version of your application.
        "2.1.0",
        # The author/manufacturer of your application.
        "Harel Ben-Attia"
    )
    return builder


# Dynamically enable automatic code signing.
def register_code_signers():
    # You will need to run with `pyoxidizer build --var ENABLE_CODE_SIGNING 1` for
    # this if block to be evaluated.
    if not VARS.get("ENABLE_CODE_SIGNING"):
        return

    # Use a code signing certificate in a .pfx/.p12 file, prompting the
    # user for its path and password to open.
    # pfx_path = prompt_input("path to code signing certificate file")
    # pfx_password = prompt_password(
    #     "password for code signing certificate file",
    #     confirm = True
    # )
    # signer = code_signer_from_pfx_file(pfx_path, pfx_password)

    # Use a code signing certificate in the Windows certificate store, specified
    # by its SHA-1 thumbprint. (This allows you to use YubiKeys and other
    # hardware tokens if they speak to the Windows certificate APIs.)
    # sha1_thumbprint = prompt_input(
    #     "SHA-1 thumbprint of code signing certificate in Windows store"
    # )
    # signer = code_signer_from_windows_store_sha1_thumbprint(sha1_thumbprint)

    # Choose a code signing certificate automatically from the Windows
    # certificate store.
    # signer = code_signer_from_windows_store_auto()

    # Activate your signer so it gets called automatically.
    # signer.activate()


# Call our function to set up automatic code signers.
register_code_signers()

# Tell PyOxidizer about the build targets defined above.
register_target("exe", make_exe)
register_target("resources", make_embedded_resources, depends=["exe"], default_build_script=True)
register_target("install", make_install, depends=["exe"], default=True)
register_target("msi_installer", make_msi, depends=["exe"])

# Resolve whatever targets the invoker of this configuration file is requesting
# be resolved.
resolve_targets()
