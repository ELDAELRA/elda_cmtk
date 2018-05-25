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

(** Crawler results filter and presenter tool.
 *
 *  The tool aggregates the report synthesis:
 *  
 *     - per site, across language pairs, yielding:
 *     
 *       - total number of TUs
 *       - total number of tokens (all languages together)
 *       - global alignment score mean and variance
 *       - global length ratio mean and variance (?)
 *
 *    - per language pair, across sites:
 *
 *      - total number of web sites
 *      - total number of TUs
 *      - total number of tokens language 1
 *      - total number of tokens language 2
 *      - global alignment score mean and variance
 *      - global length ratio mean and variance
 * *)

open Core

module CrawlerIo : sig
  type per_site_aggreg_t = {site: string; provenance: string;
                            languages: string list; token_counts: int;
                            tu_counts: int; avg_score_mean: float;
                            avg_score_var: float; avg_lratio_mean: float;
                            avg_lratio_var: float; diff_tu_ratio: float;
                            diff_orig_sites: int}

  type simple_language = {language: string; token_counts: int}

  type per_lang_pair_aggreg_t = {
    language_pair: simple_language * simple_language;
    number_of_sites: int;
    number_of_provenances: int;
    tu_counts: int;
    avg_score_mean: float;
    avg_score_var: float;
    avg_lratio_mean: float;
    avg_lratio_var: float
  }

  val read_report: ?delimiter: char -> string -> string list list

  val write_per_site_aggregates_to_csv: ?delimiter: char ->
    data: per_site_aggreg_t list -> csv_file: string -> unit -> unit

  val write_per_language_pair_aggregates_to_csv: ?delimiter: char ->
    data: per_lang_pair_aggreg_t list -> csv_file: string -> unit -> unit

  val build_confusion_matrix_from_language_pair_aggregates: data:
                                                              per_lang_pair_aggreg_t list -> string list list
end = struct

  (** Type of an aggregated entry, per site.
   * XXX: The languages should be European Union languages, not strings? *)
  type per_site_aggreg_t = {site: string; provenance: string;
                            languages: string list; token_counts: int;
                            tu_counts: int; avg_score_mean: float;
                            avg_score_var: float; avg_lratio_mean: float;
                            avg_lratio_var: float; 
                            diff_tu_ratio: float;
                            diff_orig_sites: int}

  (** Simplified language type.
   * XXX: language should be eea_language instead of string.*)
  type simple_language = {language: string; token_counts: int}

  (** Type of an aggregated entry, per language pair.*)
  type per_lang_pair_aggreg_t = {
    language_pair: simple_language * simple_language;
    number_of_sites: int;
    number_of_provenances: int;
    tu_counts: int;
    avg_score_mean: float;
    avg_score_var: float;
    avg_lratio_mean: float;
    avg_lratio_var: float
  }

  (** Interface function to read synthesis report (to mutualize with the crawler
   * manager?) *)
  let read_report ?(delimiter=';') filename =
    if Sys.file_exists_exn filename then
      Csv.load filename ~separator: delimiter
    else
      failwith (Printf.sprintf "File %s does not exist" filename)

  (** Interface function to write per-site aggregates to CSV. *)
  let write_per_site_aggregates_to_csv ?(delimiter=';') ~data ~csv_file () =
    let data' = List.map data ~f: (
        function
        | {site; provenance; languages; token_counts; tu_counts;
           avg_score_mean; avg_score_var; avg_lratio_mean; avg_lratio_var;
           diff_tu_ratio; diff_orig_sites} ->
          [site; provenance; (String.concat languages ~sep:" & ")] @
          (List.map [token_counts; tu_counts] ~f: Int.to_string) @
          (List.map [avg_score_mean; avg_score_var; avg_lratio_mean;
                     avg_lratio_var; diff_tu_ratio] 
             ~f: Float.to_string) @
          [Int.to_string diff_orig_sites]
      ) in
    let data'' = ["Web site"; "Provenance"; "Languages"; "Token counts";
                  "TU counts"; "Average alignment score mean";
                  "Average alignment score variance";
                  "Average length ratio mean";
                  "Average length ratio variance"; 
                  "Third-party sites TU ratio";
                  "Number of distinct third-party sites"] :: data' in
    Csv.save ~separator: delimiter csv_file data''

  (** Interface function to build per-language pair confusion matrix.*)
  let build_confusion_matrix_from_language_pair_aggregates ~data =
    let module Lang_pair_Comparator = Comparator.Make(
      struct
        type t = string * string [@@deriving sexp]
        let compare x y =
          match x, y with
          | (s_1, s_2), (s_3, s_4) -> compare (s_1 ^ s_2) (s_3 ^ s_4)
      end
    ) in
    let all_langs =
      List.map data ~f: (
        function
        | {language_pair = ({language = lang_1; _}, {language = lang_2; _}); _}
            -> [lang_1; lang_2]
      )
      |> List.concat
      |> String.Set.of_list
      |> String.Set.to_list in
    let data' =
      List.map data ~f: (
        function
        | {language_pair = ({language = lang_1; _}, {language = lang_2; _});
           tu_counts; _} ->
          [(lang_1, lang_1), Float.infinity;
           (lang_1, lang_2), Float.of_int tu_counts;
           (lang_2, lang_1), Float.of_int tu_counts;
           (lang_2, lang_2), Float.infinity]
      )
      |> List.concat
      |> List.dedup_and_sort
        ~compare: (Tuple2.compare ~cmp1: (Tuple2.compare
                                            ~cmp1: String.compare
                                            ~cmp2: String.compare)
                                  ~cmp2: Float.compare)
      |> Map.of_alist_exn (module struct
                            type t = string * string
                            include Lang_pair_Comparator end) in
    let dim = List.length all_langs in
    let data_a = Array.make_matrix ~dimx: dim ~dimy: dim Float.infinity in
    List.iteri all_langs ~f: (
      fun i lang_s ->
        List.iteri all_langs ~f: (
          fun j lang_t ->
            match Map.find data' (lang_s, lang_t) with
            | None -> ()
            | Some num -> data_a.(i).(j) <- num
        )
    );
    let out_data = all_langs :: (data_a |> Array.to_list |> List.map ~f:
                                    Array.to_list
                                 |> List.map ~f: (List.map ~f: (
                                   fun el ->
                                     Float.to_string el
                                     |> String.strip ~drop: ((=) '.')))) in
    List.map2_exn ("\\" :: all_langs) out_data ~f: (
      fun hdel datel -> hdel :: datel
    )

  (** Interface function to write per-language pair aggregates to CSV. *)
  let write_per_language_pair_aggregates_to_csv ?(delimiter=';')
      ~data ~csv_file () =
    let data' = List.map data ~f: (
        function
        | {language_pair = ({language = lang_1; token_counts = tok_c_1},
                            {language = lang_2; token_counts = tok_c_2});
           number_of_sites; number_of_provenances; tu_counts;
           avg_score_mean; avg_score_var; avg_lratio_mean; avg_lratio_var} ->
          [lang_1; lang_2] @
          (List.map [tok_c_1; tok_c_2; number_of_sites;
                     number_of_provenances; tu_counts] ~f: Int.to_string) @
          (List.map [avg_score_mean; avg_score_var; avg_lratio_mean;
                     avg_lratio_var] ~f: Float.to_string)
      ) in
    let data'' = ["Source language"; "Target language";
                  "Source language token counts";
                  "Target language token counts"; "Number of sites";
                  "Number of provenances"; "TU counts";
                  "Average alignment score mean";
                  "Average alignment score variance";
                  "Average length ratio mean";
                  "Average length ratio variance"] :: data' in
    Csv.save ~separator: delimiter csv_file data''
end

module CrawlerAggregator : sig

  val aggregate_data_per_site: ?full_data: string list list option -> 
    string list list -> CrawlerIo.per_site_aggreg_t list

  val aggregate_data_per_language_pair: string list list ->
    CrawlerIo.per_lang_pair_aggreg_t list

end = struct
  open CrawlerIo

  (** Helper to extract per-site data (a first form of aggregation..*)
  let extract_per_site_data datum =
    try
      match datum with
      | [site; provenance; lang_1; lang_2; l_1_count; l_2_count; tu_count;
         score_mean; score_var; lratio_mean; lratio_var] ->
        [site; provenance; [lang_1; lang_2] |> String.concat ~sep: ";"; 
         Int.(of_string l_1_count + of_string l_2_count |> to_string);
         tu_count; score_mean; score_var; lratio_mean; lratio_var]
      | _ -> let fname, lnum, cnum, _ = __POS__ in
        raise (Match_failure ("Malformed data in " ^ fname, lnum, cnum))
    with
    | Failure _ -> []

  (** Various helpers to extract bits of information from the first aggregations
   * of the entries. *)
  let extract_language_information = function
    | [_; _; langs; _; _; _; _; _; _] -> String.split langs ~on: ';'
    | _ -> []

  let extract_token_count = function
    | [_; _; _; tok_count; _; _; _; _; _] -> Int.of_string tok_count
    | _ -> 0

  let extract_tu_count = function
    | [_; _; _; _; tu_count; _; _; _; _] -> Int.of_string tu_count
    | _ -> 0

  let extract_mean_score = function
    | [_;  _; _; _; _; mean_score; _; _; _] -> Float.of_string mean_score
    | _ -> 0.

  let extract_var_score = function
    | [_;  _; _; _; _; _; var_score; _; _] -> Float.of_string var_score
    | _ -> 0.

  let extract_mean_lratio = function
    | [_;  _; _; _; _; _;  _; mean_lratio; _] -> Float.of_string mean_lratio
    | _ -> 0.

  let extract_var_lratio item = List.last_exn item |> Float.of_string

  (** Interface function to aggregate statistics, on a per-site level, across
   *  language pairs.
   *  Algorithm: group data on a per-site list, then aggregate all statistics
   *  across all language pair for a given site.*)
  let aggregate_data_per_site ?(full_data=None) data =
    let fn_aggregs data' = List.groupi data' ~break: (fun i prev next ->
        if i > 0 then
          match prev, next with
          | prev' :: _, next' :: _ -> if prev' <> next' then true else false
          | _ -> false
        else
          false) in
    let aggregs = fn_aggregs data in
    let diff_original orig_site in_site =
      let in_site' =
        Str.global_replace (Str.regexp {|^www\.|}) "" in_site in
      let ends_with pat str =
        match Pcre.extract ~rex: (Pcre.regexp (pat ^ "$")) str
        with
        | exception Not_found -> false
        | _ -> true in
      String.split orig_site ~on: ';'
      |> List.exists ~f: (Fn.non (ends_with in_site')) in
    let count_all_and_diff in_site =
      match full_data with
      | None -> 0, (0, [])
      | Some full_data' ->
        List.fold full_data' ~init: (0, (0, [])) ~f: (
          fun (prevl, (prevr, prevrlist)) ->
            function
            | cr_site :: orig_site :: _ when cr_site = in_site ->
              if diff_original orig_site cr_site then
                (prevl + 1, (prevr + 1,
                             String.split orig_site ~on: ';' @ prevrlist))
              else
                (prevl + 1, (prevr, prevrlist))
            | _ -> (prevl, (prevr, prevrlist))
        ) in
    let diff_tu_ratio_and_volume in_site =
      let all, (diff, difflist) = count_all_and_diff in_site in
      ((Float.of_int @@ diff) /.  (Float.of_int @@ all),
       List.dedup_and_sort ~compare: String.compare difflist |> List.length) in
    List.map aggregs ~f: (List.map ~f: extract_per_site_data)
    |> List.filter ~f: ((<>) [[]])
    |> List.map ~f: (fun per_site_info ->
        match per_site_info with
        | [site; provenance; _; _; _; _; _; _; _] :: _ ->
          let diff_tu_ratio, diff_orig_sites = diff_tu_ratio_and_volume site
          in
          let head = List.hd_exn per_site_info in
          {site; provenance;
           languages = 
             List.fold per_site_info
               ~init: (extract_language_information head)
               ~f: (fun prev next -> List.append
                       prev (extract_language_information next))
             |> List.dedup_and_sort ~compare: String.compare;
           token_counts =
             List.fold per_site_info
               ~init: 0
               ~f: (fun prev next -> prev + extract_token_count next);
           tu_counts =
             List.fold per_site_info
               ~init: 0
               ~f: (fun prev next -> prev + extract_tu_count next);
           avg_score_mean = List.map per_site_info ~f: extract_mean_score
                            |> Array.of_list |> Gsl.Stats.mean;
           avg_score_var = List.map per_site_info ~f: extract_var_score
                           |> Array.of_list |> Gsl.Stats.mean;
           avg_lratio_mean = List.map per_site_info ~f: extract_mean_lratio
                             |> Array.of_list |> Gsl.Stats.mean;
           avg_lratio_var = List.map per_site_info ~f: extract_var_lratio
                            |> Array.of_list |> Gsl.Stats.mean;
           diff_tu_ratio = diff_tu_ratio;
           diff_orig_sites = diff_orig_sites}
        | _ -> failwith "Error"
      )

  (** Helper to extract data per language pair. *)
  let extract_per_lang_pair_data = function
    | site ::  provenance ::  l_1 ::  l_2 :: l_1_tok :: l_2_tok :: tu_counts
      :: tail
      -> (site, provenance,
          ({CrawlerIo.language = l_1; token_counts = Int.of_string l_1_tok},
           {CrawlerIo.language = l_2; token_counts = Int.of_string l_2_tok}),
          Int.of_string tu_counts, List.map tail ~f: Float.of_string)
    | _ -> let fname, lnum, cnum, _ = __POS__ in
      raise (Match_failure ("Malformed data in " ^ fname, lnum, cnum))

  (** Interface function to aggregate statistics, on a per-language pair level,
   *  across sites.
   *  Algorithm: group data on a per-language pair list, then aggregate all
   *  statistics across all sites for a given language pair.*)
  let aggregate_data_per_language_pair data =
    let aggregs = List.sort data ~compare: (fun prev next ->
        match prev, next with
        | _ :: _ :: l_1 :: l_2 ::_, _ :: _ :: l_1' :: l_2' :: _ ->
          let l_1_2 = String.concat [l_1; l_2] ~sep: ";" in
          let l_1_2' = String.concat [l_1'; l_2'] ~sep: ";" in
          compare l_1_2 l_1_2'
        | _ -> compare prev next)
                  |> List.groupi ~break: (fun i prev next ->
                      if i > 0 then
                        match prev, next with
                        | _ :: _ :: l_1 :: l_2 :: _, _ :: _ :: l_1' :: l_2' :: _ ->
                          if String.Set.compare (String.Set.of_list [l_1; l_2])
                              (String.Set.of_list [l_1'; l_2']) <> 0 then
                            true
                          else false
                        | _ -> false
                      else
                        false) in
    List.map aggregs ~f: (
      List.map ~f: (
        fun x ->
          try
            extract_per_lang_pair_data x
          with
          | Invalid_argument _ ->
            ("", "",
             ({language = ""; token_counts = 0},
              {language = ""; token_counts = 0}),
             0, [0.])
      ))
    |> List.filter ~f: (
      function
      | ("", _, _, _, _) :: _ -> false
      | _ -> true)
    |> List.map ~f: (
      fun lpair_entries ->
        match lpair_entries with
        | (_, _, ({language = l_1}, {language = l_2}), _, _) :: _ ->
          {number_of_sites = List.length (
               List.fold lpair_entries
                 ~init: []
                 ~f: (fun prev next ->
                     match next with
                     | (site, _, _, _, _) -> List.append prev [site])
               |> String.Set.of_list |> String.Set.to_list);
           number_of_provenances = List.length (
               List.fold lpair_entries
                 ~init: []
                 ~f: (fun prev next ->
                     match next with
                     | (_, prov, _, _, _) -> List.append prev [prov])
               |> String.Set.of_list |> String.Set.to_list);
           tu_counts = List.fold lpair_entries
               ~init: 0
               ~f: (fun prev next ->
                   match next with
                   | (_, _, _, counts, _) -> prev + counts);
           avg_score_mean = List.map lpair_entries
               ~f: (fun el ->
                   match el with
                   | (_, _, _, _, score_mean :: _) -> score_mean
                   | _ -> 0.)
                            |> Array.of_list |> Gsl.Stats.mean;
           avg_score_var = List.map lpair_entries
               ~f: (fun el ->
                   match el with
                   | (_, _, _, _, _ :: score_var :: _) -> score_var
                   | _ -> 0.)
                           |> Array.of_list |> Gsl.Stats.mean;
           avg_lratio_mean = List.map lpair_entries
               ~f: (fun el ->
                   match el with
                   | (_, _, _, _, _ :: _ :: lratio_mean :: _)
                     -> lratio_mean
                   | _ -> 0.)
                             |> Array.of_list |> Gsl.Stats.mean;
           avg_lratio_var = List.map lpair_entries
               ~f: (fun el ->
                   match el with
                   | (_, _, _, _, _ :: _ :: _ :: lratio_var :: [])
                     -> lratio_var
                   | _ -> 0.)
                            |> Array.of_list |> Gsl.Stats.mean;
           language_pair = (
             {language = l_1;
              token_counts = List.fold lpair_entries
                  ~init: 0
                  ~f: (fun prev next ->
                      match next with
                      | (_, _, ({language = l_1; token_counts = toks}, _), _, _)
                        -> prev + toks)
             },
             {language = l_2;
              token_counts = List.fold lpair_entries
                  ~init: 0
                  ~f: (fun prev next ->
                      match next with
                      | (_, _, (_, {language = l_2; token_counts = toks}), _, _)
                        -> prev + toks)
             })
          }
        | _ -> failwith "Malformed term"
    )
end


module CrawlerCommand : sig
  val main: unit -> unit
end = struct
  type aggregation_type = [
    | `Per_site
    | `Per_language_pair
    | `Both
    | `None]

  (** Main command driver. Core's ppx extensions not used because of the
      historical usage of the orm package, which uses camlp4..*)
  let main () =
    let usage_msg =
      String.concat
        ~sep: "\n"
        ["Automatically aggregate and present information from the crawling \
          synthetic report";
         "=== Copyright © ELDA 2016 - All rights reserved ===\n"] in
    if Sys.argv |> Array.length < 2 then
      let errmsg = usage_msg ^ "\n" ^
                   "No argument supplied. Please supply -help or --help to \
                    see the arguments\n" in
      Printf.printf "%s" errmsg
    else
      let delimiter = ref ";" in
      let aggreg_type = ref `None in
      let report_synthesis_fname = ref "" in
      let report_full_fname = ref "" in
      let speclist = [
        ("-a", Arg.Symbol (["per-site"; "per-language-pair"; "both"],
                           (function
                             | "per-site" -> aggreg_type := `Per_site
                             | "per-language-pair" ->
                               aggreg_type := `Per_language_pair
                             | "both" -> aggreg_type := `Both
                             | at ->
                               begin
                                 Printf.printf "Wrong aggregation type: %s\n" at;
                                 aggreg_type := `None
                               end)),
         "Statistics aggregation type");
        ("-rs", Arg.Set_string report_synthesis_fname,
         "Name of the report synthesis file");
        ("-rf", Arg.Set_string report_full_fname,
         "Name of the report full file");
        ("-d", Arg.Set_string delimiter, "Delimiter of the CSV report files")
      ] in
      Arg.parse speclist (fun x -> raise (Arg.Bad ("Bad argument: " ^ x)))
        usage_msg;
      if !report_synthesis_fname = "" then
        raise (Arg.Bad (Printf.sprintf "The filename \"%s\" is not valid."
                          !report_synthesis_fname));
      let fname, ext  =
        match Filename.split_extension !report_synthesis_fname with
        | rep_syn_base, Some rep_syn_ext -> rep_syn_base, "." ^ rep_syn_ext
        | rep_syn_base, None -> rep_syn_base, ".csv" (*If no extension, then .csv*) in
      let per_site_aggregation_csv =
        String.concat [fname; "_per_site_aggregation"; ext] in
      let per_langpair_aggregation_csv =
        String.concat [fname; "_per_language_pair_aggregation"; ext] in
      let confusion_matrix_csv =
        String.concat [fname; "_per_language_pair_confusion_matrix"; ext] in
      let data' = CrawlerIo.read_report ~delimiter: (Char.of_string !delimiter)
          !report_synthesis_fname in
      match !aggreg_type with
      | `Per_site ->
        let full_data =
          if !report_full_fname = "" then
            None
          else
            Some (CrawlerIo.read_report
                    ~delimiter: (Char.of_string !delimiter)
                    !report_full_fname) in
        let data = CrawlerAggregator.aggregate_data_per_site
            ~full_data data' in
        CrawlerIo.write_per_site_aggregates_to_csv
          ~delimiter: (Char.of_string !delimiter)
          ~csv_file: per_site_aggregation_csv ~data ()
      | `Per_language_pair ->
        let data = CrawlerAggregator.aggregate_data_per_language_pair data' in
        begin
          CrawlerIo.write_per_language_pair_aggregates_to_csv
            ~delimiter: (Char.of_string !delimiter)
            ~csv_file: per_langpair_aggregation_csv ~data ();
          Csv.save
            ~separator: (Char.of_string !delimiter)
            confusion_matrix_csv
            (CrawlerIo.build_confusion_matrix_from_language_pair_aggregates
               ~data)
        end
      | `Both ->
        let full_data =
          if !report_full_fname = "" then
            None
          else
            Some (CrawlerIo.read_report
                    ~delimiter: (Char.of_string !delimiter)
                    !report_full_fname) in
        let data_s = CrawlerAggregator.aggregate_data_per_site
            ~full_data data' in
        let data_l =
          CrawlerAggregator.aggregate_data_per_language_pair data' in
        begin
          CrawlerIo.write_per_site_aggregates_to_csv
            ~delimiter: (Char.of_string !delimiter)
            ~csv_file: per_site_aggregation_csv ~data: data_s ();
          CrawlerIo.write_per_language_pair_aggregates_to_csv
            ~delimiter: (Char.of_string !delimiter)
            ~csv_file: per_langpair_aggregation_csv ~data: data_l ();
          Csv.save
            ~separator: (Char.of_string !delimiter)
            confusion_matrix_csv
            (CrawlerIo.build_confusion_matrix_from_language_pair_aggregates
               ~data: data_l)
        end
      | `None -> Printf.printf "No valid aggregation type selected. \
                                Exiting.\n"
end

let () = CrawlerCommand.main ()
