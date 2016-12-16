Software Toolkit for Crawling Management
========================================

Synopsis
--------

The ELDA Crawled Data Management Toolkit helps the users to manage the crawling process using the ILSP's Focused Crawler, [ILSPFFC](http://nlp.ilsp.gr/redmine/projects/ilsp-fc).

The toolkit consists of a set of 13 command-line tools.

The tools run on native Linux / UNIX-compatible platforms, are implemented as POSIX-like command-line tools and provide the following functionalities:

1.  Manage crawling session launching, starting from a set of URLs. Thus, the dispatching of each URL to a crawling process, the creation of an associated seed URL file and crawled data directory are handled. Also, the crawling processes (via the ILSP-FC-2.2.3) are dispatched to all the CPUs of the machine, thus all crawls are run in parallel and all processes are logged. This is implemented in the `crawler_launcher` tool.
2.  Perform crawled URL deduplication across several crawling "sessions" (i.e.) sets / batches of crawled data directories. Thus, we make sure we don't use the same crawled data several times. This is implemented in the `crawler_metacleaner` tool.
3.  Back-up crawled results to zip files, based on user-specified file extensions (e.g. tmx, txt, html, xml). This is implemented in the `crawler_backuper` tool.
4.  Analyze crawled data (currently, only output\_data\_l1-l2.tmx) and compile CSV-formatted reports. Currently, four reports are compiled:

    -   "synthesis" report, with one entry per site and language pair, which gives several statistics at this level, some extracted from the metadata found in the TMX files, some computed (e.g. alignment score means and variances across the TUs for a given language pair in a given crawled web site).
    -   "full" report, containing one entry per TU, but exposing all available information regarding that TU.
    -   per-language pair "co-occurrence" matrix, which is symmetric and lists the number of TUs for each language pair.
    -   list of unique URLs the TUs come from.

    This is implemented in the `crawler_reporter` tool.

5.  Aggregate crawled data, on a per-site basis and / or on a per-language pair basis, having per-crawled site entries aggregated across language pairs and per-language pair entries aggregated across crawled sites. This allows us to make informed decisions as to which crawled sites to analyze further, with respect to the volume of the data they hold, and to the language pair coverage. This is implemented in the `crawler_aggregator` tool.
6.  Plot per-language histograms by displaying, for each language, the number of TUs for all pairs involving that language. The plots are saved in the PDF format and can be generated even without full graphic support on the machine the tool is run on. This is implemented in the `crawler_plotter` tool.
7.  Filter crawled data, according to several criteria: threshold on the number of TUs per site \* language pair, threshold on the ratio between the alignment score variance and mean per site \* language pair (which might give us a measure of the translation quality heterogeneity), outlier detection and filtering based on the alignment scores within each web site and language pair, etc. This is implemented in the `crawler_filter` tool.
8.  Randomly sample the crawled data. Several samplings of the same TU population can be done without overlapping between the samples. This is implemented in the `crawler_sampler` tool.
9.  Pretty print the various reports (as built at 4 and 5), by selecting the rows, columns and their orders and outputting the results either in a textual format compatible with the quality validation format described in the document that we have already shared with the consortium, or in a CSV format for further processing. This is implemented in the `crawler_pprinter` tool.
10. Dump the various CSV reports (as built at 4 and 5) to SQL databases. This is implemented in the `crawler_dbdumper` tool.
11. Interact with the SQL databases (handled with PostgreSQL) via a console allowing us to perform random SQL queries, as well as to dump in a straightforward manner the selected data in a CSV format compatible with the formats generated at 4 and 5. This is implemented in the `crawler_dbretriever` tool.
12. Integrate the results of the manual TU-level quality control annotations and of the PSI validation into the TU information. This is implemented in the `crawler_qcintegrator` tool.
13. Generate TMX files based on the manual quality control information and on the PSI information. This is implemented in the `crawler_tmxbuilder` tool.

All tools are equipped with a command-line help system and with UNIX man pages. In order to get the gestalt of the toolkit, you can have a look at its synthetic [diagram](./elda_cmtk.svg).

Usage
-----

### Deployment without OPAM

In order to use the toolkit, we first need to build it. Then we need to deploy it on a host machine. This process is described in the [INSTALL](./INSTALL.md) document.

If the toolkit is deployed without OPAM, by just copying the executables and man pages, it can be used simply by launching each binary in the directory where it is placed, as:

    ./crawler_tool -help

to see the options, then as:

    ./crawler_tool 'OPTIONS'

according to the needs.

Each tool's manual page can be accessed by doing:

    man -M path/to/toolkit/man/pages/ crawler_tool

### Deployment with OPAM

If the toolkit is deployed via OPAM on the crawling machine, as described in the [INSTALL](./INSTALL.md) document, then each tool and man page is in the path, hence it suffices to do:

    crawler_tool -help

to see the options, then:

    crawler_tool 'OPTIONS'

as needed.

Each tool's manual page can be accessed by doing simply:

    man crawler_tool
