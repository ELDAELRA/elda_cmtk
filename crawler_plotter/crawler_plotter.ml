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

(** Plot language-wise per language pair histograms, starting from the
 * per-language pair aggregation information.
 *
 * *)

open Core

module CrawlerPlotter : sig 

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

  val unique_langs: per_lang_pair_aggreg_t list -> string list

  val plot_per_language_pair_tu_histogram: per_lang_pair_aggreg_t list ->
    string -> string -> unit
end = struct 

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

  (** Helper to get unique languages.*)
  let unique_langs data =
    List.map data ~f: (
      function
      | {language_pair = {language}, _} -> language)
    @
    List.map data ~f: (
      function
      | {language_pair = _, {language}} -> language)
    |> List.dedup

  (** Helper function to generate plot-related x-axis to language pair
   * mapping.*)
  let xaxis_to_language data source_language datum =
    let data' = unique_langs data |> List.filter ~f: ((<>) source_language) in
    List.Assoc.find_exn (
      List.mapi data' ~f:
        (fun i language -> Float.of_int (i + 1), language)
    ) datum ~equal: Float.equal

  (** Helper to map counts to the y-axis.*)
  let counts_to_yaxis count =
    let open Core in
    Printf.sprintf "%d TUs" (Int.of_float count)

  (** Helper to get per-language pair count.*)
  let lpairs_counts =
    let open Core in
    List.map ~f: (fun {tu_counts} -> Float.of_int tu_counts)

  (** Interface function to plot per-language pair histogram (in terms of TU
   * counts).*)
  let plot_per_language_pair_tu_histogram raw_data source_language
      plot_filename =
    let open Core in
    let module A = Archimedes in
    let data = List.filter raw_data ~f: (
        function
        | {language_pair = {language = lang_s}, {language = lang_t}}
          when List.mem [lang_s; lang_t] source_language ~equal: String.equal
            -> true
        | _ -> false) in
    let canvas = A.init ~text: 8. ["cairo"; "PDF"; plot_filename] in
    A.Viewport.xlabel canvas "Target languages";
    A.Viewport.ylabel canvas "Number of TUs";
    A.Viewport.title
      canvas
      (Printf.sprintf "Per-language pair TU distribution for source language \
                       \"%s\"" source_language);
    let xtics = A.Tics.(Equidistants ((Custom (xaxis_to_language data source_language)),
                                      0., 1., 1)) in
    let ytics = A.Tics.(Equidistants ((Custom (counts_to_yaxis)), 0., 1e+4, 10))
    in
    A.Axes.x ~tics: xtics canvas;
    A.Axes.y ~tics: ytics canvas;
    A.Viewport.set_color canvas A.Color.black;
    let xes =
      List.range 1 (List.length data) ~stop: `inclusive
      |> List.map ~f: Float.of_int in
    let yes =
      lpairs_counts data |> List.map ~f: ((+.) 1e3) in
    List.iter2_exn xes yes ~f: (
      fun x y ->
        A.Viewport.text
          ~rotate: (4. *. Float.atan 1. /. 4.) (* pi / 4 radians = 45 degrees.*)
          canvas x y Int.(to_string @@ of_float y));
    A.Array.xy
      ~style: (`Bars 0.1)
      ~fill: true
      ~fillcolor: A.Color.royal_blue
      canvas
      (List.range ~stop: `inclusive 1 (List.length data)
       |> Array.of_list
       |> Array.map ~f: Float.of_int)
      (lpairs_counts data |> Array.of_list);
    A.close canvas

end

let plotting_driver ~per_lang_aggreg_fname ?(delimiter=';') () =
  let open CrawlerPlotter in
  (** Helper to read reports.*)
  let read_report ?(delimiter=';') filename =
    if Sys.file_exists_exn filename then
      Csv.load filename ~separator: delimiter
    else
      failwith (Printf.sprintf "File %s does not exist" filename) in
  let data = read_report ~delimiter per_lang_aggreg_fname 
             |> List.filteri ~f: (fun i _ -> if i > 0 then true else false) in
  let aggregs = 
    List.map data ~f: (
      function
      | [s_lang; t_lang; s_lang_tc; t_lang_tc; number_of_sites; 
         number_of_provenances; tu_counts; avg_score_mean; avg_score_var; 
         avg_lratio_mean; avg_lratio_var] ->
        {language_pair = {language = s_lang; 
                          token_counts = Int.of_string s_lang_tc}, 
                         {language = t_lang; 
                          token_counts = Int.of_string t_lang_tc};
         number_of_sites = Int.of_string number_of_sites;
         number_of_provenances = Int.of_string number_of_provenances;
         tu_counts = Int.of_string tu_counts;
         avg_score_mean = Float.of_string avg_score_mean;
         avg_score_var = Float.of_string avg_score_var;
         avg_lratio_mean = Float.of_string avg_lratio_mean;
         avg_lratio_var = Float.of_string avg_lratio_var}
      | _ -> failwith "Ill-formed input data"
    ) in
  let per_langpair_aggregation_tu_histo =
    Str.replace_first (Str.regexp "\\.csv$") "_tu_histogram.pdf"
      per_lang_aggreg_fname in
  List.iter (unique_langs aggregs) ~f: (
    fun lang ->
      let dir_name, base_name =
        Filename.split per_langpair_aggregation_tu_histo in
      let base, ext = Filename.split_extension base_name in
      let histo_name =
        match ext with
        | None -> Filename.concat dir_name (base ^ "_" ^ lang)
        | Some ext' -> Filename.concat dir_name 
                         (base ^ "_" ^ lang ^ "." ^ ext') in
      plot_per_language_pair_tu_histogram aggregs lang histo_name
  )

let command =
  Command.basic_spec
    ~summary: "Automatically plot per-language language pair distributions of \
               TU counts"
    ~readme: (fun () -> "=== Copyright © 2016 ELDA - All rights reserved ===\n")
    Command.Spec.(
      empty
      +> flag "-A" (required string) ~doc: " Per language pair aggregation file"
      +> flag "-d" (optional_with_default ";" string)
        ~doc: " CSV delimiter (; by default)"
    )
    (fun per_lang_aggreg_fname delimiter ->
       if Fn.non Sys.file_exists_exn per_lang_aggreg_fname then
         raise (Sys_error (Printf.sprintf "Per-language pair aggregation file \
                                           %s does not exist"
                             per_lang_aggreg_fname))
       else
         plotting_driver 
           ~per_lang_aggreg_fname 
           ~delimiter: (Char.of_string delimiter)
    )

let () = Command.run ~version: "1.0" ~build_info: "ELDA on Debian" command
