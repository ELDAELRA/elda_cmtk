(**************************************************************************)
(* Copyright (C) 2016 Evaluations and Language Resources Distribution     *)
(* Agency (ELDA) S.A.S (Paris, FRANCE), all rights reserved.              *)
(* contact http://www.elda.org/ -- mailto:info@elda.org                   *)
(* author: Vladimir Popescu -- mailto:vladimir@elda.org                   *)
(*                                                                        *)
(* This file is part of the ELDA Crawling Management Toolkit (ELDA-CMTK). *)
(*                                                                        *)
(* ELDA-CMTK is free software: you can redistribute it and/or modify it   *)
(* under the terms of the GNU General Public License as published by the  *)
(* Free Software Foundation, either version 3 of the License, or (at your *)
(* option) any later version.                                             *)
(*                                                                        *)
(* ELDA-CMTK is distributed in the hope that it will be useful, but       *)
(* WITHOUT ANY WARRANTY; without even the implied warranty of             *)
(* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU      *)
(* General Public License for more details.                               *)
(*                                                                        *)
(* You should have received a copy of the GNU General Public License      *)
(* along with ELDA-CMTK, in the LICENSE file. If not, see                 *)
(* <http://www.gnu.org/licenses/>.                                        *)
(**************************************************************************)

(* Program to manage the crawling process in a transparent manner.
 * Input: List of URL files to be crawled (in CSV format).
 * Output (effects): 
   * Generate appropriate directory structure for crawling (destination
   * directories for crawling results)
   * Generate appropriate seed URL files
   * Generate appropriate commands
   * Launch the commands in parallel.
 * *)
open Core.Std

exception Not_implemented_error

module type CrawlerCommand_t = sig
  type mode
  val command: lang: string -> filter: string -> url_seed_file_name: string -> 
    dest_dir: string -> version: string -> crawl: bool -> mtlen: int ->
    unit -> string
end

module MonoCrawlerCommand = struct
  type mode = Monolingual

  let command ~lang ~filter ~url_seed_file_name ~dest_dir ~version ~crawl
              ~mtlen () =
    Printf.sprintf
      {|java -Dlog4j.configuration=file:log4j.xml -jar ilsp-fc-%s-jar-with-dependencies.jar %s -export -dedup -mtlen %d -a elda -f -type m -lang "%s" -k -filter "%s" -u "%s" -oxslt -dest "%s" -bs "%s/output_data" > "%s/log.txt" 2>&1|}
      version (if crawl = true then "-crawl" else "") mtlen lang filter
      url_seed_file_name dest_dir dest_dir dest_dir
end

module MultiCrawlerCommand = struct

  type mode = Multilingual

  let command ~lang ~filter ~url_seed_file_name ~dest_dir ~version ~crawl
              ~mtlen () =
    Printf.sprintf 
      {|java -Dlog4j.configuration=file:log4j.xml -jar ilsp-fc-%s-jar-with-dependencies.jar %s -export -dedup -pairdetect -align -tmxmerge -f -k -oxslt -type p -n 10 -t 20 -len 0 -mtlen %d  -pdm "aupdih" -lang "%s" -a elda -filter "%s" -u "%s" -dest "%s" -bs "%s/output_data" > "%s/log.txt" 2>&1|}
      version (if crawl = true then "-crawl" else "") mtlen lang filter
      url_seed_file_name dest_dir dest_dir dest_dir

end

module CrawlerManager (CrawlerCommand: CrawlerCommand_t) : sig
  exception Crawler_manager_error of string
  val parse_url_file: ?delimiter: char -> string -> string list
  val make_dir_from_url: ?root_dir: string -> string -> string
  val make_seed_url_file: ?root_dir: string -> string -> string
  val command: lang: string -> url_seed_file_name: string ->
    dest_dir: string -> version: string -> crawl: bool -> mtlen: int -> unit ->
    string
  val run_commands: urls_file_name: string -> root_dir: string -> 
    ?lang: string -> version: string -> crawl: bool -> mtlen: int -> unit ->
    unit
end = struct

  exception Crawler_manager_error of string

  (** Helper for parsing the URL CSV input file.
   * Algorithm: Look for "URL" or "url" in the column name, or else look for
   * entries starting with "http://" or with "www.", and retrieve them in a list.
   * *)
  let parse_url_file ?(delimiter=';') filename = 
    if Sys.file_exists_exn filename then
      let data = Csv.load filename ~separator: delimiter |> List.concat in
      let url_prefix = Str.regexp {|\(https?://\)\|\(www\.\)|} in
      List.filter data ~f: (fun el -> Str.string_match url_prefix el 0)
    else
      raise (Crawler_manager_error (Printf.sprintf "File %s does not exist" 
                                      filename))

  (** Helper to make root URL for directory and file creation purposes.
   * Algorithm: Drop http[s]:// and trailing /. Replace remaining / with _.
   * *)
  let make_root_url url =
    Str.global_replace (Str.regexp {|/|}) "__" (Str.split
                                                  (Str.regexp {|https?:?/*\|/$|}) url |> List.hd_exn)


  (** Helper for generating the crawling destination directory, for each seed URL.
   * Algorithm: concatenate a prefix directory with the seed URL and create the
   * directory if not already present.
   * *)
  let make_dir_from_url ?(root_dir=".") url =
    let dest_dir = Filename.concat root_dir (make_root_url url) in
    Unix.mkdir_p dest_dir;
    dest_dir

  (** Helper for creating seed URL file to feed into the crawler.
   * Algorithm: for each seed URL and root directory, create a file named
   * "url_seed_file_<seed URL>.url" into the root directory
   * *)
  let make_seed_url_file ?(root_dir=".") url =
    let root_url = make_root_url url in
    let filename = Printf.sprintf "url_seed_file_%s.url" root_url in
    let filepath = Filename.concat root_dir filename in
    Out_channel.write_all filepath ~data: url;
    filepath

  (** Actual crawler command, given by the functorized approach, by getting the
   * appropriate crawler command module.
   * *)
  let command ~lang ~url_seed_file_name ~dest_dir ~version ~crawl ~mtlen () =
    let pattern =
      Filename.parts dest_dir
      |> List.last_exn
      |> Str.split (Str.regexp {|www\.\|__|})
      |> List.hd_exn in
    let filter = String.concat 
        [".*"; Str.global_replace (Str.regexp {|\.|}) {|\.|} pattern; ".*"] in
    let url_seed_file_name' = Filename.realpath url_seed_file_name in
    let dest_dir' = Filename.realpath dest_dir in
    CrawlerCommand.command ~lang ~filter 
      ~url_seed_file_name: url_seed_file_name'
      ~dest_dir: dest_dir' ~version ~crawl ~mtlen ()

  (** Command list generator, starting from the input CSV file. This links all
   * the pieces together.
   * Algorithm:
     * Parse URL file and build URL list.
     * Iterate on the list and create:
       * crawled results directory (if absent)
       * seed URL file
       * crawler command.
     * Return crawler command list (after the effectful directory and seed URL
     * files creation).
   * *)
  let run_commands ~urls_file_name ~root_dir
      ?(lang="bg;hr;cs;da;nl;en;et;fi;fr;de;el;hu;ga;it;lv;lt;pl;pt;ro;sl;es;sv;no")
      ~version ~crawl ~mtlen () =
    let open Async.Std in
    Parmap.pariter (
      fun url ->
        let dest_dir = make_dir_from_url ~root_dir url in
        let url_seed_file_name = make_seed_url_file ~root_dir url in
        let command_str =
          command ~lang ~url_seed_file_name ~dest_dir ~version ~crawl ~mtlen ()
        in
        begin
          printf "Now running:\n%s\n" command_str;
          let progl, argsl =
            List.split_n (String.split command_str ~on: ' ') 1 in
          let prog = List.hd_exn progl in
          let args =
            List.rev argsl |> List.drop_while ~f: ((<>) ">") |> List.tl_exn
            |> List.rev in
          let output_file =
            List.drop_while ~f: ((<>) ">") argsl |> List.tl_exn |> List.hd_exn
            |> String.strip ~drop: ((=) '"') in
          let handle =
            Process.run_exn ~prog ~args ()
            >>= fun log_data -> Writer.save output_file ~contents: log_data in
          (* Force async code execution: unit Deferred.t -> unit *)
          Thread_safe.block_on_async_exn (fun () -> handle)
        end
    ) (Parmap.L (parse_url_file urls_file_name))

end

module CrawlerDriver : sig
  type eu_language =
    | Bg
    | Hr
    | Cs
    | Da
    | Nl
    | En
    | Et
    | Fi
    | Fr
    | De
    | El
    | Hu
    | Ga
    | It
    | Lv
    | Lt
    | Mt
    | Pl
    | Pt
    | Ro
    | Sk
    | Sl
    | Es
    | Sv
    | No

  type langset =
    | Subset of eu_language list
    | One of eu_language [@@deriving sexp]

  type runmode =
    | Monolingual
    | Multilingual

  val drive_commands: mode: runmode -> lang: langset -> 
    urls_file_name: string -> root_dir: string -> version: string ->
    crawl: bool -> mtlen: int -> unit -> unit
  exception CrawlerDriver_error of string
end = struct
  type eu_language =
    | Bg
    | Hr
    | Cs
    | Da
    | Nl
    | En
    | Et
    | Fi
    | Fr
    | De
    | El
    | Hu
    | Ga
    | It
    | Lv
    | Lt
    | Mt
    | Pl
    | Pt
    | Ro
    | Sk
    | Sl
    | Es
    | Sv
    | No [@@deriving sexp]

  type langset =
    | Subset of eu_language list
    | One of eu_language [@@deriving sexp]

  type runmode =
    | Monolingual
    | Multilingual [@@deriving sexp]

  exception CrawlerDriver_error of string

  (** Helper to convert langset to a string of language abbreviations.
   * *)
  let langset_to_string langset = 
    let language_to_string language = 
      sexp_of_eu_language language |> Sexp.to_string |> String.lowercase in
    match langset with
    | One language -> language_to_string language
    | Subset languages -> List.map languages ~f: language_to_string
                          |> String.concat ~sep: ";"

  (** Command driver, accepting one mode at a time.
   * *)
  let drive_commands' ~mode ~lang ~urls_file_name ~root_dir ~version
                      ~crawl ~mtlen () =
    match (mode, lang) with
    | (Monolingual, (One _ as langset)) 
    | (Monolingual, (Subset [_] as langset))-> 
      let lang' = langset_to_string langset in
      let module M = CrawlerManager(MonoCrawlerCommand) in
      M.run_commands ~urls_file_name ~root_dir ~lang: lang' ~version ~crawl
                     ~mtlen ()
    | (Multilingual,
       (Subset languages as langset)) when List.length languages > 1 -> 
      let lang' = langset_to_string langset in
      let module M = CrawlerManager(MultiCrawlerCommand) in
      M.run_commands ~urls_file_name ~root_dir ~lang: lang' ~version ~crawl
                     ~mtlen ()
    | (Multilingual, One _) | (Multilingual, Subset [_]) -> 
      raise (CrawlerDriver_error 
               "Cannot work in multilingual mode and use one single language")
    | (Monolingual, Subset languages) when List.length languages > 1 ->
      raise (CrawlerDriver_error
               "Cannot work in monolingual mode and use several languages")
    | _ -> raise (CrawlerDriver_error "Unknown mode and languages")

  let drive_commands ~mode ~lang ~urls_file_name ~root_dir ~version ~crawl
                     ~mtlen () =
    match mode with
    | Monolingual | Multilingual -> drive_commands' ~mode ~lang ~urls_file_name
                                      ~root_dir ~version ~crawl ~mtlen ()
end

let command =
  Command.basic
    ~summary: "Automatically launch the crawler"
    ~readme: (fun () -> "=== Copyright © 2016 ELDA - All rights reserved  ===\n")
    Command.Spec.(
      empty
      +> flag "-m" (required string) ~doc: " Crawler mode (mono or multi)"
      +> flag "-l" (required string) ~doc: " Languages, separated by ',', or 'all'"
      +> flag "-u" (required string) ~doc: " URL file name (in CSV format)"
      +> flag "-d" (required string) ~doc: " Root directory to put the crawler results"
      +> flag "--crawler-version" (optional_with_default "2.2.3" string)
        ~doc: " Version of the ILSP-FC crawler (2.2.3 by default)"
      +> flag "-crawl" (optional_with_default true bool)
        ~doc: " To crawl or not to crawl (true by default, i.e. the crawler
               crawls). Same as ILSP-FC's eponymous option."
      +> flag "-mtlen" (optional_with_default 0 int)
        ~doc: " ILSP-FC's mtlen option: \"Minimum number of tokens in cleaned
              document. If the length (in terms of tokens) of the cleaned text
              is less than this value, the document will not be stored.\". See
              also http://nlp.ilsp.gr/redmine/projects/ilsp-fc/wiki/Crawl.")
    (fun mode' lang' urls_file_name root_dir version crawl mtlen ->
       let open CrawlerDriver in
       let mode = 
         begin 
           match mode' with
           | "mono" -> Monolingual
           | "multi" -> Multilingual
           | _ -> raise (CrawlerDriver_error 
                           (Printf.sprintf "Unknown mode: %s" mode'))
         end in
       let lang = if lang' = "all" then
           let all_langs = [Bg; Hr; Cs; Da; Nl; En; Et; Fi; Fr; De; El; Hu; Ga;
                            It; Lv; Lt; Pl; Pt; Ro; Sl; Es; Sv; No] in
           if version > "2.2.3" then Subset (Mt :: all_langs)
           else Subset all_langs
         else
         if String.contains lang' ',' then
           String.concat ["(Subset ("; (String.split lang' ~on: ','
                                        |> List.map ~f: String.capitalize |> String.concat ~sep: " "); "))"]
           |> Sexp.of_string |> langset_of_sexp
         else
           String.concat ["(One "; (String.capitalize lang'); ")"]
           |> Sexp.of_string |> langset_of_sexp in
       drive_commands ~mode ~lang ~urls_file_name ~root_dir ~version ~crawl
                      ~mtlen)

let () = Command.run ~version: "1.0" ~build_info: "ELDA on Debian" command
