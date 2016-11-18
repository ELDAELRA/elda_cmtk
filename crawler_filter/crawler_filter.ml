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

(** This tool handles
 *
 * - report_synthesis sorting
 * - report_synthesis pruning
 * - report_full pruning by projecting report_synthesis.
 * - report_full pruning by itself.
 * - report_synthesis pruning by projecting report_full.
 *
 * *)

open Core.Std

module CrawlerFilter : sig
  val sort_synthesis_data: string list list -> string list list
  val prune_synthesis_data: tu_number: int -> ?var_mean_ratio: float ->
    string list list -> string list list
  val project_pruning_on_full_data: pruned_synthesis: string list list ->
    string list list -> string list list
  val prune_full_data: ?dev_median_ratio: float ->
    ?keep_only_official_languages: bool ->
    ?prune_different_origins: bool -> ?prune_missspellings_percent: int ->
    ?inf_lratio: float -> ?sup_lratio: float -> string list list ->
    string list list
  val project_pruning_on_synthesis_data: synthesis_data: string list list ->
    string list list -> string list list
end = struct

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

  type eea_language =
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
    | Is
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

  (** Interface function to sort entries in the report data according to the
   *  specifications listed in the preamble of this file.*)
  let sort_synthesis_data =
    List.sort ~cmp: (fun x y ->
        match x, y with
        (*don't sort site-wise!*)
        | site :: _, site' :: _ when site' <> site -> compare site site
        | [_; _; _; _; _; _; tus; _; _; _; _],
          [_; _; _; _; _; _; tus'; _; _; _; _] when tus' <> tus ->
          compare (Int.of_string tus') (Int.of_string tus)
        | [_; _; _; _; _; _; _; s_mean; _; _; _],
          [_; _; _; _; _; _; _; s_mean'; _; _; _] when s_mean' <> s_mean
          -> compare (Float.of_string s_mean') (Float.of_string s_mean)
        | x, y -> compare x y)

  (** Helper to prune the data based on the number of TUs, on the variance
   *  to mean ratio (default: inifinity) thresholds.*)
  let prune_synthesis_data ~tu_number ?(var_mean_ratio=Float.infinity) =
    List.filteri ~f: (fun i datum ->
        match datum with
        | [_; _; _; _; _; _; tus; mean; var; _; _] when i > 0 ->
          let ratio = (Float.of_string var) /. (Float.of_string mean) in
          if (Int.of_string tus) >= tu_number &&
             (Float.compare ratio var_mean_ratio) = -1 then
            true
          else
            false
        | _ when i = 0 -> true
        | _ -> false)

  let project_pruning_on_full_data ~pruned_synthesis =
    let pruned_sites = List.map pruned_synthesis ~f: (
        function
        | site :: _ -> site
        | [] -> failwith "Ill-formed entry") |> List.dedup in
    let pruned_s_langs = List.map pruned_synthesis ~f: (
        function
        | _ :: _ :: s_lang :: _-> s_lang
        | _ -> failwith "Ill-formed entry") |> List.dedup in
    let pruned_t_langs = List.map pruned_synthesis ~f: (
        function
        | _ :: _ :: _ :: t_lang :: _ -> t_lang
        | _ -> failwith "Ill-formed entry") |> List.dedup in
    List.filteri ~f: (fun i datum ->
        match datum with
        | site :: _ :: _ :: _ :: _ :: s_lang :: _ :: _ :: t_lang :: _
          when i > 0 -> List.mem pruned_sites site
                        &&
                        List.mem pruned_s_langs s_lang
                        &&
                        List.mem pruned_t_langs t_lang
        | _ when i = 0 -> true
        | _ -> false)

  (* Helper to get list of misspelled words, according to aspell for the
   * specified language.
   * If aspell is not installed on the running machine, or not available in
   * PATH, or the specified language is not installed, then the helper returns
   * the empty list.*)
  let get_misspelled_words ~data ~language =
    let open Async.Std in
    let lang =
      Printf.sprintf "--lang=%s_%s" language (String.uppercase language) in
    let stdout_path = Sexplib.Path.parse ".stdout" in
    let myproc = Process.create_exn ~prog: "aspell" ~args: ["--list"; lang] ()
    in
    let data' =
    begin
      myproc >>| Process.stdin >>= fun x -> Writer.write x data;
      myproc >>= fun x -> Process.collect_output_and_wait x
             >>| fun x -> Process.Output.sexp_of_t x
                          |> Sexplib.Path.get ~path: stdout_path
                          |> Sexp.to_string
    end in
    (* Blocking 'a Deferred.t -> 'a *)
    Thread_safe.block_on_async_exn (fun () -> data')
    |> Pcre.replace ~rex: (Pcre.regexp "[\"]") ~templ: ""
    |> Scanf.unescaped
    |> String.split ~on: '\n'
    |> List.filter ~f: ((<>) "")

  (** Function to prune full data on the deviation with respect to the median
   * ratio (threshold: infinity) and on the difference between crawled site and
   * original site(s).*)
  let prune_full_data ?(dev_median_ratio=Float.infinity)
      ?(keep_only_official_languages=false)
      ?(prune_different_origins=false)
      ?(prune_missspellings_percent=100)
      ?(inf_lratio=0.0)
      ?(sup_lratio=Float.infinity) data =
    let in_data =
      if prune_different_origins = false then data
      else
        List.hd_exn data :: List.filteri data ~f: (fun i ->
            function
            | crawled_site :: orig_sites :: _ when i > 0 ->
              let crawled_site' =
                Str.global_replace (Str.regexp {|^www\.|}) "" crawled_site in
              let ends_with pat str =
                match Pcre.extract ~rex: (Pcre.regexp (pat ^ "$")) str
                with
                | exception Not_found -> false
                | _ -> true in
              let original_sites = String.split orig_sites ~on: ';' in
              List.for_all original_sites ~f: (ends_with crawled_site')
            | _ -> false
          )
    in
    let fn_outliers ~mu data =
      let data_median = Oml.Statistics.Descriptive.median data in
      let deviation = Array.map data ~f: (
          fun el -> el -. data_median |> Float.abs) in
      let deviation_median = Oml.Statistics.Descriptive.median deviation in
      let sigma = Array.map deviation ~f: (
          fun el -> if deviation_median <> 0. then el /. deviation_median
            else 0.) in
      Array.filter_mapi sigma ~f: (
        fun i el -> if el > mu then Some data.(i) else None) in
    let data' = List.filteri in_data ~f: (fun i _ -> i > 0)
                |> List.map ~f: (fun datum -> List.nth_exn datum 9
                                              |> Float.of_string)
                |> Array.of_list in
    let outliers = fn_outliers ~mu: dev_median_ratio data' in
    let data'' = List.hd_exn data :: List.filteri in_data ~f: (
      fun i datum ->
        if i > 0 then
          let score = List.nth_exn datum 9 in
          not @@ Array.mem outliers (Float.of_string score)
        else
          false) in
    let data''' =
      if keep_only_official_languages = false then data''
      else
        let module EeaCountry_comparator = Comparator.Make(
          struct
            type t = eea_country [@@deriving sexp]
            let compare x y =
              compare
                (sexp_of_t x |> Sexp.to_string)
                (sexp_of_t y |> Sexp.to_string)
          end
        ) in
        let official_languages =
          List.zip_exn
            [Austria; Bulgaria; Belgium; Croatia; Cyprus; Czech_Republic;
             Denmark; Netherlands; United_Kingdom; Estonia; Finland;
             France; Germany; Greece; Hungary; Iceland; Ireland; Italy;
             Latvia; Lithuania; Luxembourg; Malta; Poland; Portugal;
             Romania; Slovakia; Slovenia; Spain; Sweden; Norway]
            [[De]; [Bg]; [De; Fr; Nl]; [Hr]; [El]; [Cs]; [Da]; [Nl]; [En]; [Et];
             [Fi]; [Fr]; [De]; [El]; [Hu]; [Is]; [Ga; En]; [It]; [Lv]; [Lt];
             [De; Fr]; [Mt; En]; [Pl]; [Pt]; [Ro]; [Sk]; [Sl]; [Es]; [Sv]; [No]]
          |> Map.of_alist_exn ~comparator: EeaCountry_comparator.comparator in
        (*XXX: Very similar to the eponymous function in the reporter, safe that
         * here we use option types.*)
        let country_from_tld = function
          | "at" -> Some Austria
          | "bg" -> Some Bulgaria
          | "be" -> Some Belgium
          | "brussels" -> Some Belgium
          | "hr" -> Some Croatia
          | "cy" -> Some Cyprus
          | "cz" -> Some Czech_Republic
          | "dk" -> Some Denmark
          | "nl" -> Some Netherlands
          | "uk" -> Some United_Kingdom
          | "scot" -> Some United_Kingdom
          | "wales" -> Some United_Kingdom
          | "et" -> Some Estonia
          | "fi" -> Some Finland
          | "fr" -> Some France
          | "de" -> Some Germany
          | "gr" -> Some Greece
          | "hu" -> Some Hungary
          | "is" -> Some Iceland
          | "ie" -> Some Ireland
          | "it" -> Some Italy
          | "lv" -> Some Latvia
          | "lt" -> Some Lithuania
          | "lu" -> Some Luxembourg
          | "mt" -> Some Malta
          | "pl" -> Some Poland
          | "pt" -> Some Portugal
          | "ro" -> Some Romania
          | "sk" -> Some Slovakia
          | "si" -> Some Slovenia
          | "es" -> Some Spain
          | "gal" -> Some Spain
          | "cat" -> Some Spain
          | "eus" -> Some Spain
          | "se" -> Some Sweden
          | "no" -> Some Norway
          | _ -> None in
        List.hd_exn data :: List.filter (List.tl_exn data'') ~f: (
          function
          | _ :: sites :: _ :: _ :: _ :: source_language :: _ :: _ ::
            target_language :: _ ->
            let eea_langs =
              List.map [source_language; target_language] ~f: (
                fun lang -> String.capitalize lang |> Sexp.of_string
                            |> eea_language_of_sexp) in
            let eea_s_lang, eea_t_lang =
              List.hd_exn eea_langs, List.last_exn eea_langs in
            let countries =
              String.split sites ~on: ';'
              |> List.map ~f: (
                Fn.compose country_from_tld
                           (Fn.compose List.last_exn (String.split ~on: '.')))
            in
            begin
              match countries with
              | None :: None :: [] -> true
              | Some c :: None :: [] ->
                  List.mem (Map.find_exn official_languages c) eea_s_lang
              | None :: Some c :: [] ->
                  List.mem (Map.find_exn official_languages c) eea_t_lang
              | Some c :: Some c' :: [] ->
                  List.mem (Map.find_exn official_languages c) eea_s_lang ||
                  List.mem (Map.find_exn official_languages c') eea_t_lang
              | None :: [] -> true
              | Some c :: [] ->
                  let official_langs = Map.find_exn official_languages c in
                  List.mem official_langs eea_s_lang ||
                  List.mem official_langs eea_t_lang
              | _ ->
                  failwith "Ill-formed original site information"
            end
          | _ -> failwith "Ill-formed full report data"
        ) in
    let out_data =
      if prune_missspellings_percent = 100 then data'''
      else List.hd_exn data :: Parmap.parfold (fun x succ ->
        match x with
        | _ :: _ :: _ :: source_text :: _ :: source_language :: target_text :: _
          :: target_language :: _->
            let source_missp =
              get_misspelled_words ~data: source_text ~language: source_language
            in
            if (List.length source_missp |> Float.of_int) /.
               (List.length (String.split source_text ~on: ' ')
                |> Float.of_int) *. 100. <=
               Float.of_int prune_missspellings_percent then
              let target_missp =
                get_misspelled_words
                  ~data: target_text ~language: target_language
              in
              if (List.length target_missp |> Float.of_int) /.
                 (List.length (String.split target_text ~on: ' ')
                  |> Float.of_int) *. 100. <=
                 Float.of_int prune_missspellings_percent then x :: succ
              else succ
            else succ
        | _ -> succ
      ) (Parmap.L (List.tl_exn data'')) [] (@) in
    List.filteri out_data ~f: (fun i ->
        function
        | _ :: _ :: _ :: source_text :: _ :: _ :: target_text :: _ when i > 0->
            let length_ratio =
              CamomileLibrary.UTF8.((length source_text |> Float.of_int) /.
                                    (length target_text |> Float.of_int)) in
            Float.compare inf_lratio length_ratio <= 0 &&
            Float.compare sup_lratio length_ratio >= 0
        | _ when i = 0 -> true
        | _ -> false)


  (** Function to project the full pruning on synthesis data.*)
  let project_pruning_on_synthesis_data ~synthesis_data pruned_full_data =
    let synth_hd, synth_data =
      List.(hd_exn synthesis_data, tl_exn synthesis_data) in
    let pruned_data = List.tl_exn pruned_full_data in
    synth_hd :: List.map synth_data ~f: (
      function
      | [site; prov; s_lang; t_lang; s_tc; t_tc; tu_c; tu_score_m; tu_score_v;
         tu_lr_m; tu_lr_v] as synthesis_entry ->
        let relevant_full = List.filter pruned_data ~f: (
            function
            | site' :: _ :: prov' ::  _ :: _ :: s_lang' :: _ :: _ :: t_lang'
              :: _ when site = site' && prov = prov' &&
                        s_lang = s_lang' && t_lang = t_lang' -> true
            | _ -> false
          ) in
        let tu_c' = List.length relevant_full in
        if tu_c' = Int.of_string tu_c then
          synthesis_entry
        else if tu_c' = 0 then []
        else
          let s_tc', t_tc' =
            List.fold relevant_full ~init: (0, 0) ~f: (
              fun (prev_s, prev_t) ->
                function
                | _ :: _ :: _ :: _ :: next_s :: _ :: _ :: next_t :: _ ->
                  prev_s + (Int.of_string next_s),
                  prev_t + (Int.of_string next_t)
                | _ -> prev_s, prev_t) in
          let tu_scores =
            List.map relevant_full ~f: (
              fun el -> List.nth_exn el 9 |> Float.of_string)
            |> Array.of_list  in
          let tu_score_m', tu_score_v' =
            Oml.Statistics.Descriptive.(mean tu_scores, var tu_scores) in
          let tu_lratios =
            List.map relevant_full ~f: (
              function
              | _ :: _ :: _ :: s_text :: _ :: _ :: t_text :: _->
                CamomileLibrary.UTF8.(length s_text / length t_text)
                |> Float.of_int
              | _ -> failwith "Ill-formed entry"
            )
            |> Array.of_list in
          let tu_lr_m', tu_lr_v' =
            Oml.Statistics.Descriptive.(mean tu_lratios, var tu_lratios) in
          [site; prov; s_lang; t_lang] @
          (List.map [s_tc'; t_tc'; tu_c'] ~f: Int.to_string) @
          (List.map [tu_score_m'; tu_score_v'; tu_lr_m'; tu_lr_v']
             ~f: Float.to_string)
      | _ -> failwith "Ill-formed entry"
    )
    |> List.filter ~f: ((<>) [])
end

let command =
  Command.basic
    ~summary: "Automatically sort and prune reporting data"
    ~readme: (fun () -> "=== Copyright © 2016 ELDA - All rights reserved ===\n")
    Command.Spec.(
      empty
      +> flag "-d" (optional_with_default ";" string)
        ~doc: " Delimiter for the reports CSV files"
      +> flag "-rs" (optional string) ~doc: " Synthesis report file name"
      +> flag "-rf" (optional string) ~doc: " Full report file name"
      +> flag "--tu-threshold" (optional_with_default 1 int)
        ~doc: " Prune entries with less than the specified number of TUs (1 by \
               default)"
      +> flag "--sigma-mu-threshold" (optional_with_default Float.infinity float)
        ~doc: " Prune entries with a score variance to mean ratio greater than \
               the specified value (infinity by default)"
      +> flag "--outlier-threshold" (optional_with_default Float.infinity float)
        ~doc: " Prune entries with a deviation to median ratio greater than \
               the specified value (infinity by default)"
      +> flag "--keep-only-official-languages" (optional_with_default false bool)
        ~doc: " Keep only TUs that have at least one TUV in one language which
              is an official language of the country of the original web site
              of that TUV (false by default)"
      +> flag "--prune-different-origins" (optional_with_default false bool)
        ~doc: " Prune entries with original sites which are different from the \
               crawled site (false by default)"
      +> flag "--prune-missspellings-percent" (optional_with_default 100 int)
        ~doc: " Prune TUs having at least one TUV containing strictly more than
               the specified percent of miss-spelled tokens, out of the total
               number of tokens in the TUV, according to aspell (100 by
               default). Please note that this feature requires to have
               aspell installed, along with support for the language(s)
               under scrutiny."
      +> flag "--length-ratio-range" (optional string)
        ~doc: " Prune TUs having the length ratio out of the specified range. \
               This range can be specified as [low:high], with two particular \
               cases: [:high] yields an implicit low of 0.0, and [low:] yields \
               an implicit high of infinity."
    )
    (fun delimiter' synth_fname full_fname tu_number
      var_mean_ratio dev_median_ratio keep_only_official_languages
      prune_different_origins prune_missspellings_percent lratio_range ->
      let open CrawlerFilter in
      let read_report ?(delimiter=';') filename =
        let open Core.Std in
        if Sys.file_exists_exn filename then
          Csv.load filename ~separator: delimiter
        else
          failwith (Printf.sprintf "File %s does not exist" filename) in
      let delimiter = Char.of_string delimiter' in
      let split_ext fname =
        match Filename.split_extension fname with
        | base, Some ext -> base, "." ^ ext
        | base, None -> base, ".csv" in
      let prepare_synthesis synth_fname =
        let out_synthesis_fname =
          let fname, ext = split_ext synth_fname in
          String.concat
            [fname;
             Printf.sprintf "_sorted_pruned_%d_%.2f_%.2f"
               tu_number var_mean_ratio dev_median_ratio;
             ext] in
        let out_synthesis_data =
          read_report ~delimiter synth_fname
          |> sort_synthesis_data
          |> prune_synthesis_data ~tu_number ~var_mean_ratio in
        out_synthesis_fname, out_synthesis_data in
      let prepare_full full_fname =
        let inf_lratio, sup_lratio =
          match lratio_range with
          | None -> 0.0, Float.infinity
          | Some range -> String.strip range ~drop: (
              function
              | '[' | ']' -> true
              | _ -> false)
            |> Pcre.split ~rex:(Pcre.regexp {|\s*:\s*|})
            |> List.mapi ~f: (fun i el ->
                if el = "" then
                  if i = 0 then "0."
                  else "infinity"
                else el)
            |> List.map ~f: Float.of_string
            |> (fun lr_range -> List.(hd_exn lr_range, last_exn lr_range)) in
        let full_data = read_report ~delimiter full_fname in
        let out_full_fname =
          let fname, ext = split_ext full_fname in
          String.concat
            [fname;
             Printf.sprintf "_pruned_%d_%.2f_%.2f"
               tu_number var_mean_ratio dev_median_ratio;
             ext] in
        let out_full_data =
          prune_full_data full_data ~dev_median_ratio ~prune_different_origins
                                    ~keep_only_official_languages
                                    ~prune_missspellings_percent
                                    ~inf_lratio ~sup_lratio in
        out_full_fname, out_full_data in
      (fun () -> 
         begin
           match synth_fname, full_fname with
           | None, None -> ()
           | Some synth_fname', None ->
             let out_synthesis_fname, out_synthesis_data =
               prepare_synthesis synth_fname' in
             Csv.save ~separator: delimiter out_synthesis_fname
                                            out_synthesis_data
           | None, Some full_fname' ->
             let out_full_fname, out_full_data =
               prepare_full full_fname' in
             Csv.save ~separator: delimiter out_full_fname out_full_data
           | Some synth_fname', Some full_fname' ->
             let out_synthesis_fname, out_synthesis_data =
               prepare_synthesis synth_fname' in
             let out_full_fname, out_full_data =
               prepare_full full_fname' in
             let out_full_data' =
               project_pruning_on_full_data
                ~pruned_synthesis: out_synthesis_data out_full_data in
             let out_pruned_synth_data =
               project_pruning_on_synthesis_data
                ~synthesis_data: out_synthesis_data
                out_full_data' in
             begin
               Csv.save ~separator: delimiter out_full_fname out_full_data';
               Csv.save ~separator: delimiter out_synthesis_fname
                 out_pruned_synth_data
             end
         end
      )
    )

let () = Command.run ~version: "1.0" ~build_info: "ELDA on Debian" command
