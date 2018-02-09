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

(* This tool should use the following data sources:

 * - full TUs database (via a CSV dump)
 * 
 * - manually-annotated TU samples,
 * 
 * - sources database (via a CSV dump),
 * 
 * in order to flag each TU in the full TUs database, based on the following
 * criteria:
 * 
 * - for each TU, if PSI scope of its source (original site) is "Yes" in the
 *   sources database, then append #within_PSI_scope to "Further information";
 * 
 * - if "Legal information" is not empty for the source of the TU in the sources
 *   database, then append #legal_information to "Further information";
 * 
 * - if the TU is part of a source * language pair population that has been
 *   sampled, then the #sampled tag is appended to the "Further information"
 *   field.
 * 
 * - if a #sampled TU has been manually checked, then the #manually_checked tag
 *   is appended to the "Further information" field.
 * 
 * - if a #sampled#manually_checked TU has been marked with one of the issues
 *   among:
 *   #F: free translation;
 *   #A: alignment error;
 *   #L: language identification error;
 *   #E: translation error;
 *   #T: tokenisation error;
 *   #MT: machine-translated text, the relevant tag will be appended to the
 *        "Further information" field.
 * 
 * - if the TU is not #sampled, then nothing is appended to the "Further
 *   information" field.
 * 
 * Thus, the crawler_qcintegrator tool will enrich the "Further information"
 * field with, at the minimum, nothing, and, at the maximum, a structure like:
 * 
 * different numbers in TUVs#within_PSI_scope#legal_information#sampled
 * #manually_checked#(F|A|L|E|T|MT).
 * 
 * Thus, this tool integrates various manual quality control elements. *)

(* Algorithm:
   1. Assemble and index all manually-checked TUs, in a list of records:
    
      {id: string; source_text: string; target_text: string;
       error: error_t option }
    
   2. For each TU in the input full report:

      2.1. Compute ID (MD5) and dump data to a record structure holding the MD5
           ID plus all fields of the entry (see dbdumper, reporter etc for the
           appropriate record structure).
      2.2. Based on the "id" field, search through the list built at 1:
           if nothing found, then move to the next TU;
           else if a TU found, then
             update sampled web sites index with the original web site(s) of
             that TU;
             add #sampled#manually_checked to the "Further Information" field;
             if manually-annotated error, then
               add #error to the "Further Information" field;
   3. For each TU in the input full report:
      if TU's "Further Information" field does not hold any "#" tag, then:
        if TU's original web site in the sampled web sites index, then
          add #sampled to the "Further Information" field.

*)
open Core
open Re2.Std
open Crawler_utils

(** Generic updatable Index type. *)
module type Index_t = sig
  type t
  type index_t = t list
  val index: unit -> index_t
  val add: t -> unit
  val get: int -> t
  val mem: t -> bool
  val delete: t -> unit
  val remove: int -> unit
  val empty: unit -> unit
end

(** Generic updatable Index implementation, customized fro string objects. *)
module Index : Index_t with type t = string = struct
  type t = string
  type index_t = t list
  let _index = ref []
  let index = fun () -> !_index (* Enforce "late" binding. *)
  let add dat = if not (List.mem !_index dat ~equal: String.equal) then _index := dat :: !_index
  let get i = List.nth_exn !_index i
  let mem dat = List.mem !_index dat ~equal: String.equal
  let delete dat = _index := List.filter ~f: ((<>) dat) !_index
  let remove i = _index := List.filteri ~f: (fun i' _ -> i' <> i) !_index
  let empty = fun () -> _index := []
end

(** Main module. *)
module Crawler_qcintegrator : sig
  val manage_tus: ?delimiter: char -> ?legacy_annotations: bool ->
                  ?no_alignment_score: bool ->
                  ?no_further_information: bool -> ?cols: int list option ->
                  tu_file: string -> sample_dir: string -> psi_file: string ->
                  out_file: string -> unit -> unit
end = struct
  (** Manually-annotated error type. *)
  type error_t =
    | Character_formatting_error
    | Unspecified_error
    | Alignment_error
    | Free_translation
    | Language_identification_error
    | Translation_error
    | Tokenization_error
    | Machine_translated_text [@@deriving sexp]

  type qc_entry_t = {id: string; source_text: string; target_text: string;
                     alignment_score: float option; error: error_t list option}

  (** Error annotation parser. *)
  let error_of_string in_string =
    match Re2.(find_first_exn (create_exn {|#(\S{1,2})|}) in_string
               |> split ~include_matches: true (create_exn {|#|}))
          |> List.tl_exn with
    | "#" :: "C" :: [] -> Character_formatting_error
    | "#" :: "A" :: [] -> Alignment_error
    | "#" :: "F" :: [] -> Free_translation
    | "#" :: "L" :: [] -> Language_identification_error
    | "#" :: "E" :: [] -> Translation_error
    | "#" :: "T" :: [] -> Tokenization_error
    | "#" :: "MT" :: [] -> Machine_translated_text
    | "#" :: other_tag :: []->
        failwith (Printf.sprintf "Unknown error type: %s" other_tag)
    | exception Re2.Exceptions.Regex_match_failed _ ->
        if in_string = "??" then Unspecified_error
        else failwith (Printf.sprintf "Ill-formed error flagging %s" in_string)
    | _ -> failwith (Printf.sprintf "Impossible situation for input string: %s"
                                    in_string)

  (** Helper to index all manually-sampled and quality-controlled data. *)
  let build_samples_index ~input_dir =
    let files_list = Sys.ls_dir input_dir
    |> List.filter ~f: (fun file -> Filename.check_suffix file ".txt" &&
                                    Filename.concat input_dir file
                                    |> Sys.is_file_exn) in
    Parmap.parmap (fun file ->
      Filename.concat input_dir file
      |> In_channel.read_lines
      |> List.filter ~f: ((<>) "")
      |> List.group ~break: (
        fun line ->
          Re2.matches (Re2.create_exn
                        {|^\[\S{32,32}(;\s([0-9]{1,2}\.[0-9]+|\-))?(;\s)?.*\]$|}))
      |> List.tl_exn) (Parmap.L files_list)
    |> List.concat
    |> List.filter_map ~f: (fun entry ->
        match entry with
        | header :: source_text :: target_text :: error' ->
            let error =
              match error' with
              | [] -> None
              | _ -> Some (List.map error' ~f: error_of_string) in
            begin
              match Re2.(rewrite_exn (create_exn {|[\[\]]|}) ~template: "" header
                         |> split (create_exn {|; |})) with
              | id :: alignment_score :: _ :: [] ->
                  begin
                    try
                      Some {id; source_text; target_text; error;
                            alignment_score =
                              Float.of_string alignment_score |> Some}
                    with (* The "-" case (for repetitions) *)
                    | Invalid_argument _ -> None
                  end
              | id :: [] -> if source_text = "-" && target_text = "-" then None
                            else Some {id; source_text; target_text; error;
                                       alignment_score = None}
              | _ -> None
            end
        | _ -> None
       )

  (** Shallow-typed TU. This is enough as all the information is automatically
   *  generated.*)
  type tu_t = {crawled_site: string; original_sites: string list;
               provenance: string; source_text: string; source_token_count: int;
               source_language: string; target_text: string;
               target_token_count: int; target_language: string;
               alignment_score: float; further_information: string;
               id: string}

  (** Helper to parse TU entry as yielded by the CSV file reader. *)
  let parse_tu_entry ~delimiter
                     ~cols
                     ?(legacy_annotations=false)
                     ?(no_alignment_score=false)
                     ?(no_further_information=false) entry =
    let make_id ~cols bits =
      prune_data ~cols [bits]
      |> List.hd_exn
      |> String.concat ~sep: (String.escaped "☯")
      |> Md5.digest_string |> Digest.to_hex in
    match entry with
    | crawled_site :: original_sites :: provenance :: source_text ::
      source_token_count :: source_language :: target_text :: target_token_count
      :: target_language :: alignment_score :: further_information :: []
      -> let seed_id_elements =
          match legacy_annotations, no_alignment_score,
                no_further_information with
          | true, true, true -> [source_text; target_text]
          | true, true, false -> [further_information; source_text; target_text]
          | true, false, true -> [alignment_score; source_text; target_text]
          | true, false, false -> [alignment_score; further_information;
                                   source_text; target_text]
          | false, _, _ -> entry
         in
         Some {crawled_site;
               original_sites = String.split ~on: ';' original_sites;
               provenance; source_text;
               source_token_count = Int.of_string source_token_count;
               source_language; target_text;
               target_token_count = Int.of_string target_token_count;
               target_language;
               alignment_score = Float.of_string alignment_score;
               further_information;
               id = make_id ~cols seed_id_elements}
    | _ -> None

  (** The Unknown type is basically for not-yet-handled sites. *)
  type psi_scope_t =
    | Yes
    | No
    | Unclear
    | Unknown [@@deriving sexp]

  (** Helper to extract site from URL, i.e. toto.fr from
   * http://www.toto.fr/titi/tata.html*)
  let extract_site url =
    Re2.split (Re2.create_exn {|/{2,2}|}) url
    |> List.last_exn |> Re2.split (Re2.create_exn {|/{1,1}|})
    |> List.hd_exn
    |> Re2.(rewrite_exn (create_exn {|www\.|}) ~template: "")

  (** Helper to load PSI data.*)
  let load_psi_data ?(delimiter=';') ~psi_file () =
    if Sys.file_exists_exn psi_file then
      let data = Csv.load ~separator: delimiter psi_file in
      let module StrPsiComp = Comparator.Make(
        struct
          type t = string * psi_scope_t [@@deriving sexp]
          let compare (xi, xs) (yi, ys) =
            (* Combine web site and PSI status to spot several URLs
               referring to the same web site, but with different PSIs.*)
            compare
              (xi ^ (String.of_char delimiter) ^
               (sexp_of_psi_scope_t xs |> Sexp.to_string))
              (yi ^ (String.of_char delimiter) ^
              (sexp_of_psi_scope_t ys |> Sexp.to_string))
        end) in
      List.map (List.tl_exn data) ~f: (function
        | _ :: url :: _ :: _ :: _ :: _ :: _ :: within_psi_scope :: _ ->
            let site = extract_site url in
            let psi_scope =
              try
                Sexp.of_string within_psi_scope |> psi_scope_t_of_sexp
              with
              | Failure _ -> Unknown in
            (site, psi_scope)
        | _ -> failwith "qcintegrator: Ill-formed PSI entry")
      |> Set.of_list (module struct
                        type t = string * psi_scope_t
                        include StrPsiComp end) |> Set.to_list
    else
      failwith (Printf.sprintf "qcintegrator: The file %s does not exist"
                               psi_file)

  (** Helper to annotate TUs in terms of PSI.*)
  let handle_tu_psi ~psi_data = function
    | None -> None
    | Some tu ->
        begin
          match tu with
          | {original_sites; further_information; _} ->
              let sites = List.map original_sites ~f: extract_site in
              let psi_scopes = List.map sites ~f: (fun site ->
                let psi_entry =
                  List.find psi_data ~f: (fun (site', _) -> site' = site) in
                match psi_entry with
                | None -> Unknown
                | Some el ->
                    match el with
                    | _, psi_scope -> psi_scope) in
              if List.for_all psi_scopes ~f: ((=) Yes) then
                Some {tu with further_information = tu.further_information ^
                        "#psi_" ^ (sexp_of_psi_scope_t Yes |> Sexp.to_string)}
              else if List.exists psi_scopes ~f: ((=) No) then
                Some {tu with further_information = tu.further_information ^
                        "#psi_" ^ (sexp_of_psi_scope_t No |> Sexp.to_string)}
              else if List.exists psi_scopes ~f: ((=) Unclear) then
                Some {tu with further_information = tu.further_information ^
                        "#psi_" ^ (sexp_of_psi_scope_t Unclear |> Sexp.to_string)}
              else
                Some {tu with further_information = tu.further_information ^
                        "#psi_" ^ (sexp_of_psi_scope_t Unknown |> Sexp.to_string)}
        end

  (** Helper to check if a string contains a substring. *)
  let is_substring ~substring =
    Re2.matches (Re2.create_exn (Printf.sprintf {|%s|} substring))

  (** Helper to annotate TUs in terms of manual quality control.*)
  let handle_tu_qc
    ~sampled_sites_index
    ~(samples_index: qc_entry_t list) = function
    | None -> None
    | Some tu ->
        if not (is_substring ~substring: {|#sampled(#|$)|}
                             tu.further_information) then
          let module Sampled_sites =
            (val sampled_sites_index : Index_t with type t = string) in
          let manual_sample =
            List.filter samples_index ~f: (
              fun samp -> samp.id = tu.id &&
                samp.source_text = tu.source_text &&
                samp.target_text = tu.target_text &&
                match samp.alignment_score with
                | None -> true
                | Some score -> score = tu.alignment_score)
            |> List.hd in
          begin
            match manual_sample with
            | None ->
                if List.exists tu.original_sites ~f: Sampled_sites.mem then
                  Some {tu with further_information = tu.further_information ^
                        "#sampled"}
                else
                  Some tu
            | Some sample ->
                List.iter ~f: Sampled_sites.add tu.original_sites;
                let coda =
                  "#sampled#manually_checked" ^
                  begin
                    match sample.error with
                    | None -> ""
                    | Some errors ->
                        "#" ^ (List.map errors
                                ~f: (fun error ->
                                      sexp_of_error_t error |> Sexp.to_string)
                               |> String.concat ~sep: "#")
                  end in
                Some {tu with further_information = tu.further_information ^ coda}
          end
        else
          Some tu

  (** Public function to annotate the TUs at the PSI and QC level.*)

  let manage_tus ?(delimiter=';')
                 ?(legacy_annotations=false)
                 ?(no_alignment_score=false)
                 ?(no_further_information=false)
                 ?(cols=None)
                 ~tu_file ~sample_dir ~psi_file ~out_file () =
    let data' = 
      if Sys.file_exists_exn tu_file then
        Some (Csv.load ~separator: delimiter tu_file)
      else None in
    match data' with
    | None -> Printf.printf "qc_integrator: Make sure the input TU file (full \
                             report) %s exists" tu_file
    | Some data ->
        begin
          Index.empty ();
          let samples_index = build_samples_index ~input_dir: sample_dir in
          let psi_data = load_psi_data ~delimiter ~psi_file () in
          let out_data' =
            List.map ~f: (fun entry ->
              parse_tu_entry ~delimiter ~legacy_annotations ~no_alignment_score
                             ~no_further_information ~cols entry
              |> handle_tu_psi ~psi_data
              |> handle_tu_qc
                ~sampled_sites_index: (module Index : Index_t with type t = string)
                ~samples_index
            ) (List.tl_exn data) in
          let out_data =
            (* revisit sampled sites index to make sure all TUs are accounted
               for.*)
            Parmap.parmap (
              handle_tu_qc
                ~sampled_sites_index: (module Index : Index_t with type t = string)
                ~samples_index: []) (Parmap.L out_data')
          in
          Csv.save ~separator: delimiter out_file (
            List.hd_exn data :: List.filter_map ~f: (function
              | Some dat ->
                  Some [dat.crawled_site;
                        dat.original_sites
                        |> String.concat ~sep: (Char.to_string delimiter);
                        dat.provenance; dat.source_text;
                        dat.source_token_count |> Int.to_string;
                        dat.source_language; dat.target_text;
                        dat.target_token_count |> Int.to_string;
                        dat.target_language;
                        dat.alignment_score |> Float.to_string;
                        dat.further_information]
              | None -> None)
              out_data)
        end
end

let command =
  Command.basic_spec
    ~summary: "Automatically integrate human validations and PSI validations \
              to the full TUs"
    ~readme: (fun () -> "=== Copyright © 2017 ELDA - All rights reserved ===\n")
    Command.Spec.(
      empty
      +> flag "-d" (optional_with_default ";" string)
        ~doc: " Delimiter for the reports CSV files"
      +> flag "--columns" (optional string)
        ~doc: " Columns that have been used in pretty-printing process when \
                dumping to textual format for manual validation."
      +> flag "--legacy-annotations" no_arg
        ~doc: " Treat annotations as legacy, i.e. do not use all fields in \
                computing TU identifiers; use only alignment score, further \
                information, source text and target text"
      +> flag "--no-alignment-score" no_arg
        ~doc: " Do not use the alignment score field in computing the TU \
                identifier "
      +> flag "--no-further-information" no_arg
        ~doc: " Do not use the further information field in computing the TU \
                identifier "
      +> flag "-rf" (required string)
        ~doc: " Full report CSV file with the TUs to annotate"
      +> flag "-sd" (required string)
        ~doc: " Directory holding the text files with the human-validated samples"
      +> flag "-rp" (required string)
        ~doc: " CSV file holding a dump of the sources database \
               (http://cef-at-sources.elda.org)"
      +> flag "-of" (optional string)
        ~doc: " Optional output file, in the CSV format and with the same \
               structure as -rf, with the 'Further information' column enriched \
               (if not specified, then -rf with \"annotated_\" prepended is \
               chosen)."
    )
    (fun delimiter' cols' legacy_annotations no_alignment_score
         no_further_information tu_file sample_dir psi_file out_fname' ->
      let open Crawler_qcintegrator in
      let out_file =
        match out_fname' with
        | None -> Filename.dirname tu_file ^/ "annotated_" ^
                  Filename.basename tu_file
        | Some out_fname -> out_fname in
      let cols =
        match cols' with
        | None -> None
        | Some cols -> Some (indexes_from_selection cols) in
      manage_tus
        ~delimiter: (Char.of_string delimiter')
        ~cols
        ~legacy_annotations
        ~no_alignment_score ~no_further_information
        ~tu_file ~sample_dir ~psi_file
        ~out_file
    )

let () = Command.run ~version: "1.1.1" ~build_info: "ELDA on Debian" command
