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

(* Perform reporting on the crawled data.
 * Basic algorithm (multilingual mode): traverse the directory structure
 * recursively, starting
 * from a root directory, and look for files with names starting with
 * output_data and having the .tmx extension;
 * for each of these TMX files:
 *     - extract the language pair from the file name (like
 *     output_data_eng-fra.tmx), hence (eng, fra).
 *   - look inside each file and count the occurrences of the tu XML elements
 *     (parse the XML file, extract the <tu> elements, and count them at
 *     first).
 *   - look at the metadata (<header> > <prop type =#
 *     of words in l1>) to extract the number of word tokens and types in each
 *     of the languages (source and target).
 *   - look up the full path to extract the country code (e.g. .pt, .fr etc).
 * Then, aggregate the information, by building, for each web site:
 * - a table with, on each row, a language pair, #TUs, #tokens L1, #tokens L2.
 * - a histogram of for #TUs, #tokens L1, #tokens L2
 * - a pie chart with the first 3 TUs/L1/L2 counts, and the others grouped into
 * Other.
 *
 * For the histograms and pie charts, archimedes will be used.
 * *)

open Core.Std

module CrawlerUtils : sig 
  val walk: ?depth: int -> string -> string list
  val prune_dirtree: ?prefix: string -> extension: string -> string list 
    -> string list
end = struct

  (** Utility function for recursively traversing a directory
   *  Taken from RWO / chapter 3.
   * *)
  let rec walk ?(depth=2) root =
    if depth > 0 then
      if Sys.is_file_exn ~follow_symlinks: true root then [root]
      else
        Sys.ls_dir root
        |> List.map ~f:(fun sub -> walk (Filename.concat root sub)
                           ~depth: (depth - 1))
        |> List.concat
    else [root]

  (** Helper to prune a directory tree, on prefix and extension.*)
  let prune_dirtree ?(prefix="") ~extension filelist =
    List.filter ~f: (fun entry -> Filename.check_suffix entry extension && 
                                  Filename.basename entry |>
                                  String.substr_index ~pattern: prefix = Some 0) 
      filelist
end

module CrawlerReporter : sig
  type mode =
    | Monolingual
    | Multilingual

  val extract_multilingual_data: synthesis: bool -> ?depth: int -> string
    -> string list list
  val drive_reporter: ?mode: mode -> ?synthesis: bool -> ?separator: char
    -> dir_name: string -> report_file_name: string
    -> ?unique_urls_fname: string option -> ?depth: int -> unit -> unit
    -> unit
end = struct
  (** This are the more or less standard European Union language acronyms. *)
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

  (** This is how the ILSP-FC shows the languages in the TMX file names. *)
  type raw_language =
    | Bul
    | Hrv
    | Ces
    | Dan
    | Nld
    | Eng
    | Est
    | Fin
    | Fra
    | Deu
    | Ell
    | Hun
    | Gle
    | Ita
    | Lav
    | Lit
    | Mlt
    | Pol
    | Por
    | Ron
    | Slk (* Not working *)
    | Slv
    | Spa
    | Swe
    | Nor [@@deriving sexp]

  (** Helper for converting between 'raw' language acronyms (given by the ILSP-FC)
      and the European Union language acronyms. *)
  let raw_language_to_eu raw_lang =
    try
      match Sexp.of_string raw_lang |> raw_language_of_sexp with
      | Bul -> Some Bg
      | Hrv -> Some Hr
      | Ces -> Some Cs
      | Dan -> Some Da
      | Nld -> Some Nl
      | Eng -> Some En
      | Est -> Some Et
      | Fin -> Some Fi
      | Fra -> Some Fr
      | Deu -> Some De
      | Ell -> Some El
      | Hun -> Some Hu
      | Gle -> Some Ga
      | Ita -> Some It
      | Lav -> Some Lv
      | Lit -> Some Lt
      | Mlt -> Some Mt
      | Pol -> Some Pl
      | Por -> Some Pt
      | Ron -> Some Ro
      | Slv -> Some Sl
      | Spa -> Some Es
      | Swe -> Some Sv
      | Nor -> Some No
      | _ -> None
    with
    | Sexplib.Conv.Of_sexp_error _ -> 
      failwith (Printf.sprintf "Unknown raw language %s" raw_lang)

  (** A language pair is a pair of European Union languages.*)
  type language_pair =
    | Pair of eu_language * eu_language

  (** Helper to extract language pair from the output_data_*.tmx files.
   *  Algorithm: split the basename of the output_data_*.txm file. Then extract
   *  the language pair strings, then the language pair tuple (typed).*)
  let extract_language_pair_from_string filename =
    (* if not (Sys.file_exists_exn filename) then *)
    (*   raise (Sys_error (Printf.sprintf "File %s does not exist" filename)) *)
    (* else *)
    let filename' = Filename.basename filename |> Filename.chop_extension 
                    |> String.split_on_chars ~on: ['-'; '_'] in
    let lpair = List.slice filename' (-2) 0 |> List.map ~f: String.capitalize
                |> List.map ~f: raw_language_to_eu in
    match List.hd_exn lpair, List.last_exn lpair with
    | Some source_lang, Some target_lang -> source_lang, target_lang
    | _ -> failwith "No language pair could be constituted; please check your \
                     input data"

  (** Helper to extract the TUs from a TMX file. *)
  let extract_tus filename =
    if not (Sys.file_exists_exn filename) then
      raise (Sys_error (Printf.sprintf "File %s does not exist" filename))
    else
      Xml.parse_file filename 
      |> Xml.children 
      |> List.filter ~f:(function
          | Xml.Element ("body", _, _) -> true
          | _ -> false)
      |> List.hd_exn
      |> Xml.children
      |> List.filter ~f: (function
          | Xml.Element ("tu", _, _) -> true
          | _ -> false)


  type eea_country =
    | Austria
    | Bulgaria
    | Belgium
    | Croatia
    | Cyprus
    | Czech_Republic
    | Denmark
    | Netherlands
    | United_Kingdom
    | Estonia
    | Finland
    | France
    | Germany
    | Greece
    | Hungary
    | Iceland
    | Ireland
    | Italy
    | Latvia
    | Lithuania
    | Luxembourg
    | Malta
    | Poland
    | Portugal
    | Romania
    | Slovakia
    | Slovenia
    | Spain
    | Sweden
    | Norway [@@deriving sexp]

  type multinational_organization = 
    | Eu
    | Org
    | Info
    | Com
    | Int
    | Net
    | Biz [@@deriving sexp]

  type unknown_country = [`Unknown]

  (** GADT to get the provenance, either an EEA country, or unknown. *)
  type _ provenance =
    | Country: eea_country -> eea_country provenance
    | Multinational: multinational_organization -> unknown_country provenance

  (** Helper to get the specific provenance from the input
   * *)
  let provenance_from_typed_input: type p. p provenance -> string = function
    | Country c -> sexp_of_eea_country c |> Sexp.to_string
    | Multinational m -> Printf.sprintf "Unknown (extension: %s)"
                           (sexp_of_multinational_organization m 
                            |> Sexp.to_string |> String.lowercase)

  (* Helper to get country from TLD.
   * *)
  let country_from_tld = function
    | "at" -> Austria
    | "bg" -> Bulgaria
    | "be" -> Belgium
    | "brussels" -> Belgium
    | "hr" -> Croatia
    | "cy" -> Cyprus
    | "cz" -> Czech_Republic
    | "dk" -> Denmark
    | "nl" -> Netherlands
    | "uk" -> United_Kingdom
    | "scot" -> United_Kingdom
    | "wales" -> United_Kingdom
    | "et" -> Estonia
    | "fi" -> Finland
    | "fr" -> France
    | "de" -> Germany
    | "gr" -> Greece
    | "hu" -> Hungary
    | "is" -> Iceland
    | "ie" -> Ireland
    | "it" -> Italy
    | "lv" -> Latvia
    | "lt" -> Lithuania
    | "lu" -> Luxembourg
    | "mt" -> Malta
    | "pl" -> Poland
    | "pt" -> Portugal
    | "ro" -> Romania
    | "sk" -> Slovakia
    | "si" -> Slovenia
    | "es" -> Spain
    | "gal" -> Spain
    | "cat" -> Spain
    | "eus" -> Spain
    | "se" -> Sweden
    | "no" -> Norway
    | tld -> let fname, lnum, cnum, _ = __POS__ in
      raise (Match_failure ("Unknown TLD " ^ tld ^ " in " ^ fname, 
                            lnum, cnum))

  (** Helper to get multinational type constructor from TLD*)
  let multinational_organization_from_tld tld =
    try
      String.capitalize tld |> Sexp.of_string 
      |> multinational_organization_of_sexp
    with 
    | Sexplib.Conv.Of_sexp_error _ ->
      let fname, lnum, cnum, _ = __POS__ in
      raise (Match_failure ("Unknown TLD " ^ tld ^ " in " ^ fname, lnum, cnum))


  (** Helper to get provenancefrom TLD.
   * *)
  let provenance_from_tld tld' =
    let tld = String.lowercase tld' in
    try
      Country (country_from_tld tld) |> provenance_from_typed_input
    with 
    | Match_failure _ ->
      Multinational (multinational_organization_from_tld tld) 
      |> provenance_from_typed_input
    | _ -> failwith (Printf.sprintf "Unknown TLD: %s" tld)

  (** Helper to extract web site from the full file name
   * *)
  let extract_web_site filename = 
    Filename.dirname filename |> Filename.split |> snd |> String.split ~on: '_'
    |> List.hd_exn

  (** Helper to extract TLD from web site. *)
  let extract_tld_from_web_site website =
    match Filename.split_extension website |> snd with
    | None -> "Unknown"
    | Some tld -> tld

  (** A TUV is a structure holding an extraction date, a language, a text, the
   * token count, and the (EEA) country. *)
  type tuv = {text: string; 
              token_count: int;
              language: eu_language}

  (** A TU is made of two TUVs. *)
  type tu = {original_site: string; data: tuv * tuv; alignment_score: float;
             further_information: string}

  (** Helper to extract a specified metadata attribute from a TU.*)
  let extract_metadata_attribute ~attribute tu = 
    match Xml.children tu 
          |> List.filter ~f: (
            function
            | Xml.Element ("prop", ("type", attribute')::_, _) 
              when attribute' = attribute -> true
            | _ -> false)
          |> List.hd with
    | Some Xml.Element (_, _, value::_) -> Xml.pcdata value
    | Some _ | None -> 
      let fname, lnum, cnum, _ = __POS__ in
      raise (Match_failure ("Unknown metadata attribute " ^ attribute ^ " in " ^
                            fname, lnum, cnum))

  (** Helper to parse a TU and extract:
   * - TUV in source language (with check)
   * - TUV in target language (with check).
   * A TUV has::
    * {text: string; token_count: int; language: eu_language}
   * *)
  let parse_tu tu = 
    let tuvs = Xml.children tu |> List.map ~f: (
        function
        | Xml.Element ("tuv", ({|xml:lang|}, lang)::_, 
                       Xml.Element ("seg", [], [Xml.PCData text])::_) ->
          let lang' = String.capitalize lang |> Sexp.of_string |>
                      eu_language_of_sexp
          and token_count = String.split ~on: ' ' text |> List.length in
          (Some {language = lang'; token_count; text})
        | _ -> None
      )
               |> List.filter ~f: (
                 function
                 | None -> false
                 | _ -> true)
               |> List.map ~f: (
                 function 
                 | Some tuv -> tuv
                 | None -> failwith "Impossible situation") in
    let original_site =
      try
        extract_metadata_attribute ~attribute: "site" tu
      with
      | Match_failure (exc_string, _, _) as match_failure ->
          let attribute =
            try
              Pcre.extract ~rex: (Pcre.regexp {|\s+site\s+|}) exc_string
              |> Array.to_list |> List.hd_exn |> String.strip ~drop: ((=) ' ')
            with
            | Not_found -> Printf.sprintf "%s" "unknown" in
          if attribute <> "site" then raise match_failure
          else
            List.map ["l1-url"; "l2-url"] ~f: (fun attribute ->
              extract_metadata_attribute ~attribute tu
              |> Pcre.split ~rex: (Pcre.regexp ~flags: [`EXTENDED]
                  {|(?![^/])(/)(?=[^/])|})
              |> (fun parts -> List.slice parts 0 3)
              |> String.concat
              |> String.strip ~drop: ((=) '/'))
            |> String.concat ~sep: ";"
    in
    let alignment_score = extract_metadata_attribute ~attribute: "score" tu
                          |> Float.of_string in
    let further_information =
      try
        extract_metadata_attribute ~attribute: "info" tu
      with
      | Match_failure _ -> "" in
    {original_site; data = (List.hd_exn tuvs, List.last_exn tuvs);
     alignment_score; further_information}

  (** Type the quantitative TU metadata so that the aggregation function (see
   * below) can just work. *)
  type quantitative_tu_metadata =
    | Score
    | Length_ratio

  (** Helper to extract all TU quantitative scores and compute arithmetic mean
   * and variance.*)
  let extract_quantitative_measure ~measure ?(variance=false) tus =
    let parameter ~attribute ~variance tus' = 
      let module M = Gsl.Stats in
      let measure' x = if variance = true then M.variance x ~mean: (M.mean x)
        else M.mean x in
      List.map tus' ~f: (fun tu -> extract_metadata_attribute ~attribute tu 
                                   |> Float.of_string)
      |> Array.of_list |> measure' in
    match measure with
    | Score -> parameter ~attribute: "score" ~variance tus
    | Length_ratio -> parameter ~attribute: "lengthRatio" ~variance tus

  type tu_set_counts = {source_token_count: int;
                        target_token_count: int;
                        tu_count: int}

  (** Extract token counts for each output_data_*.tmx. *)
  let extract_token_counts =
    List.fold 
      ~init: {source_token_count = 0; target_token_count = 0; tu_count = 0}  
      ~f: (fun {source_token_count; target_token_count; tu_count} tu 
            -> match parse_tu tu with
              | {data = ({token_count = s_tok}, {token_count = t_tok})} 
                -> {source_token_count = source_token_count + s_tok; 
                    target_token_count = target_token_count + t_tok; 
                    tu_count = tu_count + 1})

  (** Main data extractor. Algorithm: 
    * get all output_data_*.tmx files from a specified directory.
    * for each file:
      * parse it and extract the TUs;
      * for each TU extract the typed TU information.
      * extract the crawled website and the provenance.
      * extract aggregated statistics: 
        * in synthesis mode:
          * source and target languages,
          * number of TUs, 
          * number of tokens in the source and target languages
        * in the non-synthesis mode: statistics at the TU level:
          * text (source and target)
          * TUV lengths (source and target)
          * original web site the TU was extracted from.
   * *)
  let extract_multilingual_data ~synthesis ?(depth=2) dirname =
    let files = CrawlerUtils.(
        walk ~depth dirname
        |> prune_dirtree ~prefix: "output_data" ~extension: "tmx") in
    let header = 
      if synthesis = true then
        ["Crawled web site"; "Provenance"; "Source language"; 
         "Target language"; "Source token count"; "Target token count"; 
         "TU count"; "TU alignment score mean"; "TU alignment score variance";
         "TU length ratio mean"; "TU length ratio variance"] 
      else
        ["Crawled web site"; "Original web site"; "Provenance"; "Source text";
         "Source token count"; "Source language"; "Target text"; 
         "Target token count"; "Target language"; "Alignment score";
         "Further information"] in
    let data = List.map files ~f: (
        fun filename -> 
          let crawled_website = extract_web_site filename in
          let provenance = extract_tld_from_web_site crawled_website 
                           |> provenance_from_tld in
          let tus = extract_tus filename in
          let stringify_language lang = sexp_of_eu_language lang |> Sexp.to_string 
                                        |> String.lowercase in
          if synthesis = false then
            List.map tus ~f: (
              fun tu -> match parse_tu tu with
                | {original_site = orig_website; 
                   data = ({text = s_text; token_count = s_tokount; 
                            language = s_lang},
                           {text = t_text; token_count = t_tokount; 
                            language = t_lang});
                   alignment_score; further_information}
                  -> let s_s_lang = stringify_language s_lang in
                  let s_t_lang = stringify_language t_lang in
                  [crawled_website; orig_website; provenance; s_text; 
                   Int.to_string s_tokount; s_s_lang; t_text;
                   Int.to_string t_tokount; s_t_lang;
                   Float.to_string alignment_score;
                   further_information]
            )
          else
            let score_mean = extract_quantitative_measure ~measure: Score tus in
            let score_var = extract_quantitative_measure ~measure: Score 
                ~variance: true
                tus in
            let len_ratio_mean = extract_quantitative_measure ~measure: Length_ratio 
                tus in
            let len_ratio_var = extract_quantitative_measure ~measure: Length_ratio
                ~variance: true
                tus in
            let {source_token_count; target_token_count; 
                 tu_count} = extract_token_counts tus in
            let s_lang, t_lang = extract_language_pair_from_string filename in
            let s_s_lang = stringify_language s_lang in
            let s_t_lang = stringify_language t_lang in
            List.concat [[crawled_website; provenance; s_s_lang; s_t_lang]; 
                         List.map [source_token_count; target_token_count; tu_count]
                           ~f: Int.to_string; 
                         List.map [score_mean; score_var; len_ratio_mean; 
                                   len_ratio_var] 
                           ~f: (fun x -> Printf.sprintf "%.2f" x)]::[]
      ) in
    let data' = List.concat data in
    header::data'

  (** Helper to extract unique original URLs, but with the relaxation wrt the
      crawled site.
      Algorithm: for each crawled site:
                 - extract its "synset"
                 - choose a synset representative: the crawled site itself if
                   it is in the synset, else the shortest synset member, domain
                   field-wise;
                 - extract is "complement", i.e. third-party sites which have
                   nothing to do with the crawled site.
  *)
  let extract_unique_urls ~full_data =
    let ends_with pat str =
      match Pcre.extract ~rex: (Pcre.regexp (pat ^ "$")) str
      with
      | exception Not_found -> false
      | _ -> true in
    let grouped_data =
      List.map (List.tl_exn full_data) ~f:(
        function
        | site :: o_sites :: _ ->
          let orig_sites =
            Pcre.split ~rex: (Pcre.regexp {|[:;/]|https?|}) o_sites
            |> List.filter ~f: ((<>) "") in
          List.map orig_sites ~f: (fun o_s -> [site; o_s])
        | _ -> failwith "Ill-formed data.")
      |> List.concat |> List.dedup |>
      List.sort ~cmp: (fun prev next ->
        match prev, next with
        | site :: _, site' :: _ -> compare site site'
        | x, y -> compare x y)
      |> List.group ~break: (fun prev next ->
          match prev, next with
          | site :: _, site' :: _ -> site <> site'
          | x, y -> false
         ) in
    let partitioned_data = List.map grouped_data ~f: (fun per_crawled_group ->
        let crawled_site = (List.hd_exn @@ List.hd_exn per_crawled_group) in
        let crawled_site' =
          Pcre.replace ~rex: (Pcre.regexp {|^www\.|}) ~templ: "" crawled_site in
        match List.partition_map per_crawled_group ~f: (
                function
                | _ :: orig_site :: _ ->
                   if ends_with crawled_site' orig_site then `Fst crawled_site
                   else `Snd orig_site
                | _ -> failwith "Ill-formed data") with
        | synset, complement ->
          let min_len' =
            List.map synset ~f: (
              Fn.compose List.length (String.split ~on: '.' ))
            |> List.min_elt ~cmp: Int.compare in
          match min_len' with
          | Some min_len ->
            (crawled_site,
             (List.filter synset ~f: ((=) crawled_site) @
              List.filter synset ~f: (
                fun el -> (String.split el ~on: '.' |> List.length) = min_len)
              |> List.hd_exn) :: [],
             complement)
          | None -> (crawled_site, [], complement)) in
    List.map partitioned_data ~f: (function
      | crawled_site, synset_repr, complements -> synset_repr @ complements)
    |> List.concat |> List.dedup

  type mode =
    | Monolingual
    | Multilingual

  (** Reporter driver, to be used in the CLI.*)
  let drive_reporter 
      ?(mode=Multilingual) ?(synthesis=true) ?(separator=';') 
      ~dir_name ~report_file_name ?(unique_urls_fname=None) ?(depth=2) () () =
    match mode with
    | Monolingual -> failwith "Monolingual reporter not implemented yet"
    | Multilingual ->
      let csv_data = extract_multilingual_data ~synthesis ~depth dir_name in
      begin
        Csv.save ~separator report_file_name csv_data;
        if synthesis = false then
          let unique_orig_urls = extract_unique_urls ~full_data: csv_data in
          match unique_urls_fname with
          | None -> ()
          | Some url_fname ->
            let out_fname = if Filename.is_implicit report_file_name then
                Filename.concat
                  (Filename.realpath dir_name) url_fname
              else
                Filename.concat
                  (Filename.dirname report_file_name)
                  url_fname in
            Out_channel.write_lines out_fname unique_orig_urls
      end
end

let command =
  Command.basic
    ~summary: "Automatically gather reporting information from the crawled data"
    ~readme: (fun () -> "=== Copyright © 2016 ELDA - All rights reserved  ===\n")
    Command.Spec.(
      empty
      +> flag "-m" (optional_with_default "multi" string) 
        ~doc: " Crawler mode (mono or multi)"
      +> flag "-s" (optional bool) ~doc: " Run in synthesis mode (enabled by default)"
      +> flag "-d" (optional_with_default ";" string) 
        ~doc: " CSV delimiter (; by default)"
      +> flag "-D" (required string) ~doc: " Root directory where the crawling
      results are"
      +> flag "-R" (required string) ~doc: " Report directory name"
      +> flag "-M" (optional string)
        ~doc: " Optional metadata to add to the report file name"
      +> flag "--maxdepth" (optional_with_default Int.max_value int)
        ~doc: " Search depth for the TMX files (very big by default).")
    (fun mode' synthesis delimiter' dir_name report_dirname metadata maxdepth ->
       let open CrawlerReporter in
       let mode = 
         begin 
           match mode' with
           | "mono" -> Monolingual
           | "multi" -> Multilingual
           | _ -> failwith (Printf.sprintf "Unknown mode: %s" mode')
         end in
       let today = Date.format (Date.today ~zone: Time.Zone.utc) "%d%m%Y" in
       let kind =
         match synthesis with
         | Some false -> "full"
         | None | Some true -> "synthesis" in
       let report_basename =
         match metadata with
         | Some meta -> Printf.sprintf "report_%s_%s_%s.csv" kind meta today
         | None -> Printf.sprintf "report_%s_%s.csv" kind today in
       let report_name =
         begin
           let endswith input pattern =
             match Pcre.extract
                     ~rex: (Pcre.regexp (Printf.sprintf "%s$" pattern)) input
             with
             | exception Not_found -> false
             | _ -> true in
           if not (endswith report_dirname "[/]+") then
             failwith "The report directory should end with '/'";
           if Filename.is_implicit report_dirname then
             Filename.concat (Filename.realpath dir_name)
             @@ Filename.concat report_dirname report_basename
           else
             Filename.concat report_dirname report_basename
         end in
       let delimiter = Char.of_string delimiter' in
       match synthesis with
       | None -> drive_reporter ~mode ~synthesis: true ~separator: delimiter
                   ~dir_name ~report_file_name: report_name
                   ~depth: maxdepth ()
       | Some synth ->
         let unique_urls_fname =
           begin
             match metadata with
             | Some meta ->
               Some (Printf.sprintf "unique_original_urls_%s_%s.txt" meta today)
             | None -> Some (Printf.sprintf "unique_original_urls_%s.txt" today)
           end
         in
         drive_reporter
           ~mode
           ~synthesis: synth
           ~separator: delimiter ~dir_name
           ~report_file_name: report_name
           ~unique_urls_fname
           ~depth: maxdepth ())

let () = Command.run ~version: "1.0" ~build_info: "ELDA on Debian" command
