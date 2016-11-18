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

(** Tool to perform sampling of TU-related data. Algoritm: start from the full
 * report, TU-wise, providing crawled and original web site, language pair
 * information and the rest of the metadata (cf. crawler_reporter) and sample
 * the TU population, according to:
 *
 * - one language pair or, by default, all language pairs;
 * - one or several web sites (URLs) or, by default, all web sites.
 *
 * The sampling process is customized with respect to:
 *
 * - the percentage of the TU population to be sampled, within each site and
 *   each language pair (the same percentage for each (site, language pair)
 *   tuple..
 *
 * - the number of samplings to be performed, without repetition.
 *
 * A the end, the output consists of:
 *
 * - a CSV file akin to the full report, but containing the samples only.
 *
 * - a pretty-printed output, as an S-expression.
 * *)

open Core.Std

module CrawlerSampler : sig
  val build_population_from_full_report: ?delimiter: char -> string ->
    string list list * string list list
  val filter_population: ?web_sites: string list option ->
    ?crawled_sites: string list option ->
    ?language_pair: (string * string) option -> string list list ->
    string list list
  val sample_tus_driver: ?delimiter: char ->
    ?out_fheader: string list list option -> ?repetitions: int -> size: int ->
    data: string list list -> out_fname: string -> unit -> unit
end = struct

  (** Interface function to read synthesis report (to mutualize with the crawler
   * manager?) *)
  let build_population_from_full_report ?(delimiter=';') filename =
    if Sys.file_exists_exn filename then
      Csv.load filename ~separator: delimiter
      |> List.partition_tf ~f: (
        function
        | site :: _ when site = "Crawled web site" -> true
        | _ -> false
      )
    else
      failwith (Printf.sprintf "File %s does not exist" filename)

  (** Function to filter the TU population to be sampled, by web site(s) and / or
   * language pair. *)
  let filter_population ?(web_sites=None) ?(crawled_sites=None)
      ?(language_pair=None) data =
    let ends_with pat str =
      match Pcre.extract ~rex: (Pcre.regexp (pat ^ "$")) str
      with
      | exception Not_found -> false
      | _ -> true in
    let strip_prefix site =
      Pcre.replace ~rex: (Pcre.regexp {|^www\.|}) ~templ: "" site in
    let data' =
      match crawled_sites with
      | None -> data
      | Some crawled_sites -> List.filter data ~f: (
          function
          | _ :: orig_sites :: _ ->
            let orig_sites' = String.split orig_sites ~on: ';' in
            List.exists crawled_sites ~f: (
              fun cr_site -> List.for_all orig_sites' ~f: (
                fun site -> ends_with (strip_prefix cr_site) site))
          | _ -> failwith "Ill-formed data"
        ) in
    let data'' =
      match web_sites with
      | None -> data'
      | Some web_sites -> List.filter data' ~f: (
          function
          | _ :: orig_sites :: _ ->
            if String.split orig_sites ~on: ';'
               |> List.exists ~f: (fun orig_site ->
                   List.mem web_sites orig_site) then true else false
          | _ -> false
        ) in
    match language_pair with
    | None -> data''
    | Some (lang_s, lang_t) -> List.filter data'' ~f: (
        function
        | _ :: _ :: _ :: _ :: _ :: s_lang :: _ :: _ :: t_lang :: _ ->
          if s_lang = lang_s && t_lang = lang_t
          || s_lang = lang_t && t_lang = lang_s then true else false
        | _ -> false
      )

  (** Function to index the population to be sampled, from 1 to the population
   * size.*)
  let index_population data =
    List.range ~stop: `inclusive 1 (List.length data)

  (** Helper sampler function.*)
  let sample ~size data =
    let size' = if size > 0 then size else size + 1 in
    try
      List.slice
        (List.permute data ~random_state: (Random.State.make_self_init ()))
        0 size'
    with
    | Invalid_argument _ ->
      begin
        (* Printf.printf "Sample size %d too big: taking all %d TUs\n" *)
        (*               size  (List.length data); *)
        data
      end

  (** Helper to group TU population by web site, then by language pair.
   *  Data needs to be sorted out first, first by web site, then by language
   *  pair.*)
  let group_data data =
    let data' = List.sort data ~cmp: (fun x y ->
        match x, y with
        | _ :: site_x :: _ :: _ :: _ :: s_lang_x :: _ :: _ :: t_lang_x :: _,
          _ :: site_y :: _ :: _ :: _ :: s_lang_y :: _ :: _ :: t_lang_y :: _ ->
          if site_x = site_y then
            compare (s_lang_x, t_lang_x) (s_lang_y, t_lang_y)
          else
            compare site_x site_y
        | _ -> 0
      ) in
    let same_site x y = match x, y with
      | _ :: site_x :: _, _ :: site_y :: _ -> site_x = site_y
      | _ -> false in
    let same_language_pair x y = match x, y with
      | _ :: _ :: _ :: _ :: _ :: s_lang_x :: _ :: _ :: t_lang_x :: _,
        _ :: _ :: _ :: _ :: _ :: s_lang_y :: _ :: _ :: t_lang_y :: _ ->
        s_lang_x = s_lang_y && t_lang_x = t_lang_y
      | _ -> false in
    List.group data' ~break: (fun x y -> not (same_site x y))
    |> List.map ~f: (
      List.group ~break: (fun x y -> not (same_language_pair x y)))

  module type Cache_t = sig
    type t = int list list list
    exception CacheError of string
    val store: t option ref
    val update: t -> unit
    val empty: unit -> unit
  end

  module Cache : Cache_t = struct
    type t = int list list list

    exception CacheError of string

    let store = ref None

    let update newcache =
      match !store with
      | None -> store := Some newcache
      | Some store' ->
        if List.equal store' newcache ~equal: (
            fun x y -> List.length x = List.length y) then
          store := Some (List.map2_exn store' newcache ~f: (
              fun s n -> List.map2_exn s n ~f: (
                  fun s' n' -> List.append s' n')))
        else
          raise (CacheError "The new cache is incompatible with the store!")

    let empty () =
      match !store with
      | None -> ()
      | Some _ -> store := None
  end

  (** Function to sample the data on their indexes (for the repetition aspects.
   *  These indexes will be bookkept in a cache and updated on each repetition.*)
  let sample_indexed_population ~cache ~size data =
    let module Cache = (val cache : Cache_t) in
    let cache = !Cache.store in
    let grouped_data = group_data data in
    let percent_of_size alist size =
      let gamut = List.length alist in
      Float.(of_int size /. 100. *. (of_int gamut)
             |> round_down |> to_int) in
    match cache with
    | Some cache' -> List.map2_exn cache' grouped_data ~f: (
        List.map2_exn ~f: (
          fun cachel datel ->
            let indexes =
              index_population datel
              |> List.filter ~f: (fun el -> not (List.mem cachel el)) in
            let size' = percent_of_size indexes size in
            sample indexes ~size: size'
        )
      )
    | None -> List.map grouped_data ~f: (
        List.map ~f: (
          fun datel ->
            let indexes = index_population datel in
            let size' = percent_of_size indexes size in
            sample indexes ~size: size'
        )
      )

  (** Function to perform several samplings of a specified size, without
   * repetition. *)
  let sample_tus_driver ?(delimiter=';') ?(out_fheader=None)
      ?(repetitions=1) ~size ~data ~out_fname () =
    let data_len = List.length data in
    let grouped_data = group_data data in
    let module C = Cache in
    let current_repetition = ref 1 in
    let total_output_len = ref 0 in
    begin
      C.empty ();
      let csv_dump = Array.create ~len: repetitions [] in
      while !current_repetition <= repetitions do
        let sample_indexes =
          sample_indexed_population ~cache: (module C) ~size data in
        begin
          C.update sample_indexes;
          Printf.sprintf ">> Repetition %d:\n" !current_repetition
          |> Core_extended.Color_print.bold
          |> Core_extended.Color_print.blue
          |> Printf.printf "%s";
          let output_data =
            List.map2_exn sample_indexes grouped_data ~f: (
              fun idx_list_list datum_list_list ->
                List.map2_exn idx_list_list datum_list_list ~f: (
                  fun idx_list datum_list ->
                    List.filteri datum_list ~f: (
                      fun i _ ->
                        if List.mem idx_list (i + 1) then true else false))) in
          let output_len =
            (List.length @@ Fn.compose List.concat List.concat output_data) in
          begin
            let o_data =
              match out_fheader with
              | None -> output_data
              | Some out_fheader' ->
                [out_fheader'] :: output_data
                @ if repetitions = 1 then
                  []
                else
                  [[[Array.(create
                              ~len: (List.length (List.hd_exn
                                                    out_fheader'))
                              "-"
                            |> to_list)]]]
            in
            let c_csv_dump = Fn.compose List.concat List.concat o_data in
            csv_dump.(!current_repetition - 1) <- c_csv_dump;
            (*XXX: Should avoid re-saving a bigger and bigger data volume
             * over and over again from one extraction to another.*)
            Csv.save ~separator: delimiter out_fname
              (Array.to_list csv_dump
               |> List.concat
               |> List.filteri ~f: (fun i ->
                   function
                   | "Crawled web site" :: _ when i > 0 -> false
                   | _ -> true));
            Printf.sprintf
              ">>>> Sample of %d percents, i.e. %d TUs from a population of %d \
               TUs from the above URL.\n"
              size
              output_len
              (data_len - !total_output_len)
            |> Core_extended.Color_print.green
            |> Printf.printf "%s";
            total_output_len := !total_output_len + output_len;
            Printf.sprintf
              "<< End of repetition %d\n" !current_repetition
            |> Core_extended.Color_print.bold
            |> Core_extended.Color_print.blue
            |> Printf.printf "%s";
          end
        end;
        incr current_repetition;
      done;
      C.empty ();
    end
end

let command =
  Command.basic
    ~summary: "Automatically sample TU-level reporting information"
    ~readme: (fun () -> "=== Copyright © 2016 ELDA - All rights reserved ===\n")
    Command.Spec.(
      empty
      +> flag "-R" (required string) ~doc: " Full report file name"
      +> flag "-d" (optional_with_default ";" string)
        ~doc: " CSV delimiter (; by default)"
      +> flag "-s" (optional string) ~doc: " List of original web sites, \
                                            separated by ,"
      +> flag "-c" (optional string) ~doc: " List of crawled web sites, \
                                            separated by ,"
      +> flag "-l" (optional string) ~doc: " Language pair, as e.g. \"en,fr\""
      +> flag "-r" (optional_with_default 1 int)
        ~doc: " Number of sampling repetitions (1 by default)"
      +> flag "-p" (required int)
        ~doc: " Sample size (percentage of samples per site * language pair)"
    )
    (fun report_fname delimiter' web_sites' crawled_sites' language_pair'
         repetitions size ->
       let open CrawlerSampler in
       let delimiter = Char.of_string delimiter' in
       let header, data' =
         build_population_from_full_report ~delimiter report_fname in
       let web_sites =
         match web_sites' with
         | None -> None
         | Some sites -> Some (String.split ~on: ',' sites) in
       let crawled_sites =
         match crawled_sites' with
         | None -> None
         | Some sites -> Some (String.split ~on: ',' sites) in
       let language_pair =
         match language_pair' with
         | None -> None
         | Some lpair ->
           let lpair' = String.split ~on: ',' lpair in
           Some (List.hd_exn lpair', List.last_exn lpair') in
       let data =
         filter_population ~language_pair ~web_sites ~crawled_sites data' in
       let out_fname =
         let dirn, base = Filename.split report_fname in
         let basen, ext =
           match Filename.split_extension base with
           | basen', None -> basen', "csv"
           | basen', Some ext' -> basen', ext' in
         let timestamp =
           Time.format (Time.now ()) "%d%m%Y_%Hh%Mm%Ss" ~zone: (Time.Zone.local)
         in
         let out_fname' =
           Printf.sprintf
             "%s_sample_%d_percent_%d_repetitions_%s.%s"
             basen size repetitions timestamp ext in
         Filename.concat dirn out_fname' in
      begin
        Printf.sprintf
          "> Sampling URL(s): %s crawled sites, %s original sites and language \
           pair %s:\n"
          (match crawled_sites' with
           | None -> ""
           | Some _cr_sites -> _cr_sites)
          (match web_sites' with
           | None -> ""
           | Some _sites -> _sites)
          (match language_pair' with
           | None -> ""
           | Some _lpair -> _lpair)
        |> Core_extended.Color_print.bold
        |> Core_extended.Color_print.cyan
        |> Printf.printf "%s";
        sample_tus_driver ~repetitions ~size ~data ~out_fname
          ~out_fheader: (Some header) ~delimiter
      end
    )

let () = Command.run ~version: "1.0" ~build_info: "ELDA on Debian" command
