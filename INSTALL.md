Installation and Deployment Instructions
========================================

Introduction
------------

The ELDA Crawling Data Management Toolkit ("ELDA-CMTK") is written in [OCaml](http://ocaml.org) 4.03 and beyond. In this document we present all details needed to compile and install the tools on a crawling machine.

These notes assume a Linux [Debian](https://www.debian.org/)-derived distribution, such as Debian itself (Jessie and beyond), [Ubuntu](http://www.ubuntu.com/), [Mint](https://www.linuxmint.com/), etc, but should be applicable, with minor modifications, to other Linux / UNIX-based distributions as well.

System Requirements
-------------------

At the operating system level, building the toolkit requires installing and setting up OCaml and a few dependencies.

-   First, install OCaml:

        apt-get install ocaml

-   Second, install [OPAM](http://opam.ocaml.org), the OCaml package manager:

        apt-get install opam

-   Third, update to OCaml 4.03:

        opam switch 4.03.0

-   Fourth, to be able to build the toolkit, install [OASIS](http://oasis.forge.ocamlcore.org/):

        opam install oasis

Software Dependencies
---------------------

The software dependencies are documented in the toolkit's `_oasis` file for all tools, or on each tool's specific `_oasis` file, situated in each tool's directory.

In order to build all tools at once, you need to:

-   Install some further system packages:

        apt-get install asciidoc libcairo2 libcairo2-dev make patchelf postgresql \
        sqlite3 libgsl0-dev libgsl0ldlbl libblas3 liblapack3 libgfortran-4.9-dev \
        libgfortran3 liblapack-dev zip

    -   `asciidoc` is needed for generating the user UNIX man pages.
    -   `libcairo2` and `libcairo2-dev` are needed for compiling and linking the `crawler_plotter` tool.
    -   `make` is needed for compiling the toolkit's binaries.
    -   `postgresql` and `sqlite3` are needed for taking advantage of the `crawler_dbdumper` and `crawler_dbretriever` tools.
    -   `patchelf` is needed for updating the path to custom shared libraries for the `crawler_plotter` tool when running on a machine without graphics support.
    -   `libgsl0-dev` and `libgsl0ldbl` are needed for the GSL-based numerics capabilities (the `gsl` OCaml package - see below).
    -   `libblas3`, `liblapack3`, `` libgfortran3`, ``libgfortran-4.9-dev`,`libgfortran3`and`liblapack-dev`are needed by the`oml\`\` OCaml package (see below).
    -   `zip` is needed by the `crawler_backuper` tool for backing up crawling results.
-   In order to benefit from spell-checking support, `aspell` needs to be installed, along with support for EEA languages, via:

        apt-get install aspell aspell-bg aspell-cs aspell-da aspell-de aspell-el \
        aspell-en aspell-es aspell-et aspell-fr aspell-ga aspell-hr aspell-hu \
        aspell-is aspell-it aspell-lt aspell-lv aspell-nl aspell-no aspell-pl \
        aspell-pt aspell-pt-br aspell-pt-pt aspell-ro aspell-sk aspell-sl aspell-sv

    and, for Finnish and Maltese, via:

        wget ftp://ftp.gnu.org/gnu/aspell/dict/mt/aspell-mt-0.50-0.tar.bz2

    and:

        wget ftp://ftp.gnu.org/gnu/aspell/dict/fi/aspell6-fi-0.7-0.tar.bz2

    and, after un-tarring the archives, by going in each directory and doing:

        ./configure && make && sudo make install

-   Install OCaml-specific dependencies:

        opam install archimedes cairo2 core core_extended ppx_jane camomile csv \
        gsl orm oml pcre parmap postgresql xml-light async re2 re cohttp

    -   `archimedes` and `cairo2` are needed for compiling the `crawler_plotter` binary.
    -   `camomile` is needed for Unicode awareness.
    -   Jane Street Capital's `core` and `ppx_jane` are basic and pervasively-necessary libraries, especially [Core](https://github.com/janestreet/core) which is a drop-in industrial-strength replacement of the OCaml's standard library.
    -   Jane Street Capital's `async` library is mostly used for asynchronous launching of subprocesses, in the `crawler_launcher` tool for launching the ILSP-FC crawler, and in the `crawler_filter` tool, for launching the `aspell` program.
    -   Jane Street Capital's `core_extended` is needed by the `crawler_sampler` tool.
    -   `csv` is needed for CSV format reading and writing capabilities.
    -   `gsl` and `oml` are needed for numerics and statistics capabilities.
    -   `orm` is required for providing Object-Relational Mapping (ORM) capabilities to the SQLite backend of the `crawler_qcintegrator` tool.
    -   `pcre` is a more powerful regular expression engine than the standard library's `str`. Most notably, it supports look-behind and look-ahead patterns.
    -   `re2` and `re` are regular expression engines. The first consists in bindings to Google's re2 engine and is provided by Jane Street Capital and follows the conventions of the `core` library. The second one provides, through the `re.glob` sub-package, UNIX name globbing capabilities.
    -   `xml-light` is needed by the `crawler_reporter` and `crawler_tmxbuilder` tool for reading, parsing and generating the XML TMX files produced by the [ILSP-FC](http://nlp.ilsp.gr/redmine/projects/ilsp-fc) crawler.
    -   `parmap` is needed for parallel computing capabilities. It is currently used by the `crawler_launcher` and `crawler_backuper` tools.
    -   `cohttp` is needed for serving contents over HTTP. It is currently used by the `crawler_localserver` tool.
    -   `tyxml` and `tyxml-ppx` are needed for programmatically generating HTML documents. They are used by the `crawler_localserver` tool.

    Please note that several OCaml dependencies require system dependencies, which can be seen, in case of installation failure, with the `depext` tool, which should be installed first:

        opam install depext

    Then, when an OPAM package fails to install, you can do:

        opam depext package

    and OPAM will attempt to install, requiring sudo access if needed, the missing system packages.

Build Process
-------------

### Development Mode

For building and using the toolkit in development mode, the OASIS tool suffices.

The toolkit building process involves at least one step: building the native executables, and at most three steps: building the executables, building the API documentation, and building the user manual pages. These steps need to be preceded by a makefile generation step, as documented below:

1.  First, the makefiles need to be generated:

        oasis setup -setup-update dynamic

2.  Second, the build can proceed, to build the software:

        make

    Thus, for each tool, a symbolic link `crawler_tool.native`, pointing to the `_build/crawler_tool.native` native executable is produced.

3.  Third, to build the documentation:

        make doc

    Thus, a symbolic link `elda_cmtk.docdir`, pointing to the `_build/elda_cmtk.docdir` directory containing the HTML API documentation is created. This is only needed when wishing to use the tools in the toolkit as library modules to build further OCaml tools. The documentation can be consulted by going to this directory and launching an HTTP server, e.g.:

        python -m SimpleHTTPServer <port>

4.  Fourth, the user UNIX man pages for all tools can be built using the `a2x` toolchain from [ASCIIDOC](http://www.methods.co.nz/asciidoc/), thus:

        for file in $(find . -type f -path "*/doc/*.1.txt"); do
            a2x -d manpage --format manpage $file  $(dirname $file); 
        done

    The resulting `crawler_tool.1` files in each tool's `doc` directory are the man pages.

As a bonus feature, in the toolkit's Git repository there is a DOT diagram summarizing the structure and functions of the toolkig, elda\_cmtk.dot, which can be built to PNG or PDF or other formats supported by Dot / Graphviz, as e.g.:

    dot -Tpdf -o elda_cmtk.pdf elda_cmtk.dot

To do this, you need to install Graphviz:

    apt-get install graphviz

### User Mode

For installing the toolkit in "user" mode, i.e. for testing purposes on a machine which has the OCaml ecosystem installed, OPAM suffices. More specifically, in order to install the executables, the man pages, and the HTML API documentation, a single command suffices:

    opam pin add elda_cmtk </path/to/elda_cmtk/repository> -y

Cleanup Process
---------------

### Developer Mode

In developer mode, should you want to clean the build results up, two steps are necessary, at most:

1.  Clean the OCaml native executables and API documentation:

        make clean && make distclean

2.  Clean the man pages:

        for file in $(find . -type f -path "*/doc/*.1"); do
            rm -f $file; 
        done

### User Mode

In user mode, it suffices to do either:

    opam remove elda_cmtk

or:

    opam pin remove elda_cmtk

The first command only uninstalls the toolkit, but maintains the repository for subsequent synchronization (via `opam update`) and installation, via `opam install elda_cmtk`.

The second command uninstalls the toolkit and removes the OPAM repository altogether, thus cleaning up all traces of the toolkit installation.

Deployment on a Crawling Machine
--------------------------------

### Without OPAM

After the toolkit has been installed in developer mode and the native executables and the user manual pages have been built, several steps are necessary for deploying the tools on a remote machine. The remote machine must use a compatible Linux distribution, i.e. a Debian or a Debian derivative.

1.  First, in order for the `crawler_plotter` tool to work without graphic support on the crawling machine, we need to:
    -   get the shared dependencies of the binary into a directory:

            mkdir -p lib && cp ``ldd crawler_plotter.native`` | cut -d ' ' -f3`` lib

    -   remove from `lib` the `libc`, `libpthread`, `libm`, `libdl` dynamic libraries which should be already present on the crawling machine
    -   patch the `DT_RUNPATH` of the binary AND of `libcairo.so.2` using the `patchelf` tool:

            patchelf --set-rpath path/to/lib/on/the/crawling/machine \
            _build/crawler_plotter.native

            patchelf --set-rpath path/to/lib/on/the/crawling/machine \
            lib/libcairo.so.2

    -   copy `lib` to the crawling machine:

            scp -r lib user@remote.server:/path/to/lib/on/the/crawling/machine

2.  Second, the native binaries can be copied to a directory on the remote machine, via SSH/SCP:

        for native in $(find _build -type f -not -type l -name "*.native"); do
            scp $native user@remote.server:/directory;
        done

3.  Third, the user manual pages can be copied in the same way:

        for file in $(find . -type f -path "*/doc/*.1"); do
            scp $file user@remote.server:/directory/man1; 
        done

4.  In order to take full benefit from the database interaction features provided by the `crawler_dbdumper` and `crawler_dbretriever` tools, PostgrSQL needs to be set-up and configured on the crawling machines, with the following constraints:
    -   the currently-logged user needs to have the rights to perform `CREATE TABLE`, `INSERT` and `SELECT` SQL queries on four databases, named `synthesis_data`, `full_data`, `per_site_aggregation_data` and `per_language_pair_aggregation_data`.

5.  Finally, in order for the process to be complete from the crawler launching, the java compiler needs to be installed on the crawling machine, and a JAR of version 2.2.3 or above of the ILSP-FC crawler is needed.

### With OPAM

If the user wishes to, she or he can install OCaml, OPAM and the 4.03.0 version of OCaml on the crawling machine:

    sudo apt-get install ocaml
    sudo apt-get install opam
    opam init
    opam install oasis
    opam pin add elda_cmtk </path/to/elda_cmtk/repository> -y

Then, step 1 described above for `crawler_plotter` involving libraries manipulation and the usage of the `patchelf` tool are still needed, unless graphics support is installed on the crawling machine.

