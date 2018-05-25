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

(* The purpose of the crawler_tmxbuilder tool is to generate TMX files
 from full reports, enriched by the crawler_qcintegrator tool from 
 manually-validated sample TUs. 

 The full report entries will be aggregated per site AND language pairs. 

 For a given set of sites and set of language pairs, a set of 
 TMX files, one per site and language pair, are created. 

 Each TMX file has the following metadata: 

 - [all ILSP-FC-provided attributes]: will be copied as such 

 - DSI (if available) 

 - number of TUs 

 - SHA-2 of the TU contents 

 - crawled web site. 

 Each TMX has the following data: 

 - TUs. 

 Each TU has the following metadata: 

 - [all fields provided by ILSP-FC, except for the alignment score, the length 
 ratio and the site / l1,2-url]: will be copied as such 

 - manually checked: Yes / No 

 - if > inferior threshold && < superior threshold: percentage of tokenization 
 errors in the sample of % size 

 - if > inferior threshold && < superior threshold: percentage of alignment 
 errors in the sample of % size 

 - if > inferior threshold && < superior threshold: percentage of translation 
 errors in the sample of % size 

 - if > inferior threshold && < superior threshold: percentage of language 
 identification errors in the sample of % size 

 - if > inferior threshold && < superior threshold: percentage of 
 machine-translated TUs in the sample of % size 

 - alternatively, if any of the above four error types is above the superior 
 threshold, no sampling information is provided and all TUs from that site 
 are discarded. 

 - alternatively, if all of the above four error types are below the inferior 
 threshold, no sampling information is provided and only the TUs containing the 
 errors are discarded. 

 - if free translation => relevant TUs are flagged with a TU-level metadata 
 attribute. 

 - if the TU comes from a web site with the psi status different from "Yes", 
 then the TU is not kept. 

 Each TU has the following data: TUVs 

 Each TUV has the following metadata: 

 - language 

 - token count 

 - original web site URL 

 Each TUV has the following data: 

 - text.

 The following DTD will be used for validating the generated files:

<!ELEMENT tmx (header,body)>
<!ATTLIST tmx
  version CDATA #REQUIRED>

<!ELEMENT header (prop)+>
<!ATTLIST header
  adminlang CDATA #REQUIRED
  creationdate CDATA #REQUIRED
  creationtool CDATA #REQUIRED
  creationtoolversion CDATA #REQUIRED
  datatype CDATA #REQUIRED
  o-tmf CDATA #REQUIRED
  segtype CDATA #REQUIRED
  srclang CDATA #REQUIRED>

<!ELEMENT body (tu)+>

<!ELEMENT tu (prop+,tuv+)>
<!ATTLIST tu
  tuid CDATA #REQUIRED>

<!ELEMENT tuv (prop+,seg)>
<!ATTLIST tuv
  xml:lang CDATA #REQUIRED>

<!ELEMENT seg (#PCDATA)>

<!ELEMENT prop (#PCDATA)>
<!ATTLIST prop
  type CDATA #REQUIRED>

The following algorithm will be run:

  0. For a given crawled web site and language pair, extract all relevant TUs
     (typed for convience);

  1. Generate XML structure, with the ILSP-FC header and relevant fields:

     adminlang; creationdate in the "yyyy-mm-ddTH:M:S+TZ" format; creationtool;
     creationtoolversion; datatype; o-tmf="al"; segtype="block" srclang in the
     ISO-639-1 codes.

     and sub-elements (prop types "l1", "l2", "lengthInTUs"; "# of words in l1";
     "# of words in l2"; "# of unique words in l1"; "#of unique words in l2"

  2. Add the following prop types to the header: "SHA-2": SHA-522 of the whole
     file contents; "DSI": unknown if not available; typed if available;
     "crawled web site": web site given to the crawler for generating this TMX
     file.

  3. For each TU entry in the full database, index it in the natural numbers (
    N* ) set and set the tuid attribute accordingly. The following fields will
    be inserted (prop types): "info": the "Further information" column, before
    the first "#"; "manually checked": Yes / No; "Free translation": Yes / No
    (only on manually-checked TUs) / Unknown; "Percentage of translation
    errors"; "Percentage of alignment errors"; "Percentage of tokenization
    errors"; "Percentage of machine-translated TUs"; "Sample size": % of total
    number of TUs, if any of the previous percentages <> N/A.

  4. For each TUV, set the "xml:lang" attribute and add the following prop
     types: "# of words:"; "site". Then, insert the "seg" attribute.
      
 
*)

open Core
open Re2.Std

(** Extend Core.List with index function à la Python. *)
module List : sig
  include module type of List
  val index: 'a list -> 'a -> int option
end = struct
  include List
  let index my_list element =
    List.filter_mapi my_list ~f: (
      fun i el -> if el = element then Some i else None)
    |> List.hd
end

module Crawler_tmxbuilder : sig
  type language_t = [
    | `Bg
    | `Hr
    | `Cs
    | `Da
    | `Nl
    | `En
    | `Et
    | `Fi
    | `Fr
    | `De
    | `El
    | `Hu
    | `Ga
    | `It
    | `Lv
    | `Lt
    | `Mt
    | `Pl
    | `Pt
    | `Ro
    | `Sk
    | `Sl
    | `Es
    | `Sv
    | `No] [@@deriving sexp]

  val driver: source_language: language_t -> target_language: language_t ->
    ?delimiter: char -> input_filename: string -> output_filename: string ->
    ?crawled_site: string option -> ?th_inf_alignment_error: float option ->
    ?th_sup_alignment_error: float option ->
    ?th_inf_translation_error: float option ->
    ?th_sup_translation_error: float option ->
    ?th_inf_tokenization_error: float option ->
    ?th_sup_tokenization_error: float option ->
    ?th_inf_language_identification_error: float option ->
    ?th_sup_language_identification_error: float option ->
    ?th_inf_machine_translated_text: float option ->
    ?th_sup_machine_translated_text: float option ->
    ?th_inf_character_formatting_error: float option ->
    ?th_sup_character_formatting_error: float option -> unit -> unit
end = struct

  type trivalent_t =
    | Yes
    | No
    | Unknown [@@deriving sexp]

  type error_type_t =
    | Character_formatting_error
    | Free_translation
    | Alignment_error
    | Tokenization_error
    | Language_identification_error
    | Machine_translated_text
    | Translation_error [@@deriving sexp]

  type error_status_t =
    | Very_likely
    | Likely
    | Unlikely
    | Undetermined [@@deriving sexp]

  (** EEA languages, as polymorphic variant for a more concise notation outside
   * the main module. *)
  type language_t = [
    | `Bg
    | `Hr
    | `Cs
    | `Da
    | `Nl
    | `En
    | `Et
    | `Fi
    | `Fr
    | `De
    | `El
    | `Hu
    | `Ga
    | `It
    | `Lv
    | `Lt
    | `Mt
    | `Pl
    | `Pt
    | `Ro
    | `Sk
    | `Sl
    | `Es
    | `Sv
    | `No] [@@deriving sexp]

  type dsi_t =
    | ECulture
    | EHealth
    | EProcurement
    | EJustice
    | Online_dispute_resolution
    | Open_data_portal
    | Safer_internet
    | EESSI
    | Other [@@deriving sexp]

  type tuv_header_t = {
    language: language_t; token_count: int; site: string} [@@deriving sexp]

  type tuv_t = tuv_header_t * string list

  type tu_header_t = {
    manually_checked: bool; free_translation: trivalent_t; tuid: int;
    errors: error_type_t list; further_information: string;
    summary: (error_type_t * error_status_t) list} [@@deriving sexp]

  type tu_t = tu_header_t * tuv_t list

  type segtype_t =
    | Block
    | Paragraph
    | Sentence
    | Phrase [@@deriving sexp]

  type tmx_header_t = {
    adminlang: language_t; creationdate: Time.t; crawled_site: string option;
    creationtool: string; creationtoolversion: string;
    datatype: string; o_tmf: string; segtype: segtype_t;
    srclang: language_t; distributor: string; description: string;
    availability: trivalent_t; l1: language_t; l2: language_t;
    length_in_tus: int; nb_words_in_l1: int; nb_words_in_l2: int;
    dsi: dsi_t; nb_uniq_words_in_l1: int; nb_uniq_words_in_l2: int}
    [@@deriving sexp]

  type tmx_t = tmx_header_t * tu_t list

  type proptype_t =
    | Availability of trivalent_t
    | Distributor of string
    | Description of string
    | L1 of language_t
    | L2 of language_t
    | Length_in_tus of int
    | Nb_words_in_l1 of int
    | Nb_words_in_l2 of int
    | Nb_of_uniq_words_in_l1 of int
    | Nb_of_uniq_words_in_l2 of int
    | Info of string
    | Sha_2 of string
    | Crawled_site of string option
    | Dsi of dsi_t
    | Manually_checked of bool
    | Free_translation of trivalent_t
    | Token_count of int
    | Site of string
    | Errors of (error_type_t * error_status_t) list [@@deriving sexp]

  type _ proptype_set_t =
    | Tmx: tmx_t -> tmx_t proptype_set_t
    | Tu: tu_t -> tu_t proptype_set_t
    | Tuv: tuv_t -> tuv_t proptype_set_t

  let lower_camel_case input =
    let bits = String.split input ~on: '_' in
    (List.hd_exn bits |> String.lowercase) ::
    List.map (List.tl_exn bits) ~f: String.capitalize
    |> String.concat

  let proptype_t_to_xml value =
    let ptypes_vals =
      match value with
      | Errors errors -> List.map errors ~f: (
        fun (error_type, error_status) ->
          sexp_of_error_type_t error_type |> Sexp.to_string |> lower_camel_case
          |> Printf.sprintf "%ss",
          sexp_of_error_status_t error_status |> Sexp.to_string)
      | _ ->
          let ptype', pval' =
            String.slice (sexp_of_proptype_t value
                          |> Sexp.to_string_hum
                          |> (fun el -> match value with
                                        | L1 _ | L2 _ -> String.lowercase el
                                        | _ -> el)) 1 (-1)
            |> String.lsplit2_exn ~on: ' ' in
          let ptype'' = lower_camel_case ptype' in
          let pval'' =
            String.substr_replace_all pval' ~pattern: "\"" ~with_: ""
            |> Re2.rewrite_exn (Re2.create_exn {|[()]|}) ~template: ""
          in
          (ptype'', pval'') :: []
    in
    List.map ptypes_vals ~f: (
      fun (ptype, pval) ->
        Xml.Element ("prop", ["type", ptype], [Xml.PCData pval]))

  let proptypes_from_node: type p. p proptype_set_t -> proptype_t list = function
    | Tmx (header, _) ->
        begin
          match header with
          | {distributor; description; availability; l1; l2;
             length_in_tus; nb_words_in_l1; nb_words_in_l2; nb_uniq_words_in_l1;
             nb_uniq_words_in_l2; crawled_site; dsi; _} ->
               [Crawled_site crawled_site;
                Distributor distributor; Description description;
                Availability availability; L1 l1; L2 l2;
                Length_in_tus length_in_tus; Dsi dsi;
                Nb_words_in_l1 nb_words_in_l1; Nb_words_in_l2 nb_words_in_l2;
                Nb_of_uniq_words_in_l1 nb_uniq_words_in_l1;
                Nb_of_uniq_words_in_l2 nb_uniq_words_in_l2]
        end
    | Tu (header, _) ->
        begin
          match header with
          | {manually_checked; free_translation; further_information; summary;
             _} -> [Manually_checked manually_checked;
                    Free_translation free_translation;
                    Info further_information; Errors summary]
        end
    | Tuv (header, _) ->
        begin
          match header with
          | {token_count; site; _} ->
              [Token_count token_count; Site site]
        end

  type attribute_t =
    | Tuid of int
    | Version of string
    | Adminlang of language_t
    | Creationdate of string (* For formatting reasons, to coincide with ILSP-FC.*)
    | Creationtool of string
    | Creationtoolversion of string
    | Datatype of string
    | O_tmf of string
    | Segtype of segtype_t
    | Xml'lang of language_t
    | Srclang of language_t [@@deriving sexp]

  let attribute_t_to_value value =
    let attr, aval =
      String.slice (sexp_of_attribute_t value |> Sexp.to_string_hum) 1 (-1)
      |> String.lsplit2_exn ~on: ' ' in
    let attr' =
      String.lowercase attr |> String.tr ~target: '_' ~replacement: '-'
      |> String.tr ~target: '\'' ~replacement: ':' in
    let aval' =
      String.lowercase aval |> String.substr_replace_all ~pattern: "\"" ~with_:""
    in
    (attr', aval')

  let element_to_xml ?(children=[]) ~element attributes =
    Xml.Element (element, List.map attributes ~f: attribute_t_to_value, children)

  type _ attribute_set_t =
    | Tmx: tmx_t -> tmx_t attribute_set_t
    | Tu: tu_t -> tu_t attribute_set_t
    | Tuv: tuv_t -> tuv_t attribute_set_t

  let attributes_from_node: type p. p attribute_set_t -> attribute_t list =
    function
      | Tmx (header, _) ->
          begin
            match header with
            | {adminlang; creationdate; creationtool; creationtoolversion;
               datatype; o_tmf; segtype; srclang; _} ->
                 [Adminlang adminlang;
                  Creationdate
                    (Time.format creationdate "%Y-%m-%dT%H:%M:%S%z"
                                 ~zone: Time.Zone.utc);
                  Creationtool creationtool;
                  Creationtoolversion creationtoolversion;
                  Datatype datatype; O_tmf o_tmf; Segtype segtype;
                  Srclang srclang]
          end
      | Tu ({tuid; _}, _) -> [Tuid tuid]
      | Tuv ({language; _}, _) -> [Xml'lang language]

  let extract_tus ?(delimiter=';') ~source_language ~target_language
                  ?(crawled_site=None) data_file =
    let raw_tus =
      try
        Some (Csv.load ~separator: delimiter data_file
              |> List.tl_exn |> List.filter ~f: (
                  function
                    | crawled_site' :: _ :: _ :: _ :: _ :: s_lang :: _ :: _ ::
                      t_lang :: _ ->
                        (if crawled_site <> None then
                          Some crawled_site' = crawled_site
                         else true) &&
                         s_lang =
                           (sexp_of_language_t source_language |> Sexp.to_string
                            |> String.lowercase) &&
                         t_lang =
                           (sexp_of_language_t target_language |> Sexp.to_string
                            |> String.lowercase)
                    | _ -> false))
      with
      | Sys_error _ | Failure _ -> None in
    let parsed_tus =
      match raw_tus with
      | None -> []
      | Some tus ->
          List.filter_mapi tus ~f: (fun i ->
            function
              | crawled_site :: original_sites :: _ :: s_text :: s_tok_count ::
                s_lang :: t_text :: t_tok_count :: t_lang :: _ :: further_info ::
                [] ->
                  if not (Re2.matches (Re2.create_exn {|#psi_Yes(#|$)|})
                            further_info) then None
                  else
                    let finfo_bits = String.split further_info ~on: '#' in
                    let further_information = List.hd_exn finfo_bits in
                    let manually_checked =
                      List.exists finfo_bits ~f: (
                        Re2.matches (Re2.create_exn {|^manually_checked$|})) in
                    let free_translation =
                      if not manually_checked then Unknown
                      else if List.exists finfo_bits ~f: (
                        Re2.matches (Re2.create_exn (Printf.sprintf {|^%s$|}
                              (sexp_of_error_type_t Free_translation
                               |> Sexp.to_string)))) then Yes
                      else No in
                    let o_sites = String.split original_sites ~on: ';' in
                    let s_site, t_site =
                      begin
                        match o_sites with
                        | s_site :: t_site :: [] -> s_site, t_site
                        | s_site :: [] -> s_site, s_site
                        | _ -> "", ""
                      end in
                    let errors =
                      match List.index finfo_bits "manually_checked" with
                      | None -> []
                      | Some index ->
                          let finfo_len = List.length finfo_bits in
                          List.slice finfo_bits (index + 1) finfo_len
                          |> List.filter ~f: (
                            fun err_string ->
                              let err_sexp = Sexp.of_string err_string in
                              try
                                error_type_t_of_sexp err_sexp <> Free_translation
                              with
                              | Sexplib.Conv.Of_sexp_error _ -> false)
                          |> List.map ~f: (
                              Fn.compose error_type_t_of_sexp Sexp.of_string) in
                    Some ({manually_checked; tuid = i + 1; free_translation;
                           summary = [];
                           further_information; errors},
                           [{language = s_lang |> String.capitalize |>
                                        Sexp.of_string |> language_t_of_sexp;
                             token_count = Int.of_string s_tok_count;
                             site = s_site}, 
                            [s_text];
                            {language = t_lang |> String.capitalize |>
                                        Sexp.of_string |> language_t_of_sexp;
                             token_count = Int.of_string t_tok_count;
                             site = t_site},
                             [t_text]])
              | _ -> None) in parsed_tus

  let get_error_ratios_from_tus ?(error_type=None) tus =
    let checked_tus =
      List.filter tus ~f: (
        function
          | {manually_checked; _}, _ -> manually_checked = true) in
    let errorn_tus =
      List.count checked_tus ~f: (function | {errors; _}, _ ->
        List.exists errors ~f: (fun error -> Some error = error_type))
    in
    (errorn_tus |> Float.of_int) /.
    (List.length checked_tus |> Float.of_int)

  let get_status_from_ratio ?(th_inf=None) ?(th_sup=None) ratio =
    match th_inf, th_sup with
    | Some th_inf', Some th_sup' when ratio > 0. ->
        if th_inf' < ratio && ratio <= th_sup' then Likely
        else if ratio <= th_inf' then Unlikely
        else Very_likely
    | _ when ratio = 0. -> Unlikely
    | _ -> Undetermined


  let enrich_and_prune_tus ~ths_inf ~ths_sup tus =
    let aggregs =
      let strip_prefix site =
        Re2.split (Re2.create_exn {|[/.]|}) site
        |> List.rev |> Fn.flip List.take 2 |> List.rev
        |> String.concat ~sep: "." in
      List.sort tus ~compare: (fun prev next ->
        match prev, next with
        |  (_, ({site = site_p; _}, _) :: _),
           (_, ({site = site_n; _}, _) :: _) ->
            String.compare (strip_prefix site_p) (strip_prefix site_n)
        | _ -> 0)
      |> List.group ~break:(fun prev next ->
          match prev, next with
          |  (_, ({site = site_p; _}, _) :: _),
             (_, ({site = site_n; _}, _) :: _)
             when strip_prefix site_p <> strip_prefix site_n -> true
          | _ -> false) in
    let thresholds =
      List.zip_exn ths_inf ths_sup
      |> List.filter ~f: (
        function
          | (error_type, Some th_inf), (error_type', Some th_sup)
            when error_type = error_type' -> true
          | _ -> false
      ) in
    List.map aggregs ~f: (fun agg ->
      let error_statuses = List.map thresholds ~f: (
        function
          | (err_t, th_inf), (_, th_sup) ->
              err_t,
              get_error_ratios_from_tus agg ~error_type: (Some err_t)
              |> get_status_from_ratio ~th_inf ~th_sup
          | _ -> .) in
      List.map agg ~f: (
        function
          | tu_hd, tu -> {tu_hd with summary = error_statuses}, tu)
      |> List.filter ~f: (
        function
          | {errors; _}, _ when errors <> [] -> false
          | {summary: _}, _ when List.exists summary ~f: (
            function
              | _, Very_likely -> true
              | _ -> false) -> false
          | _ -> true)
      )
    |> List.concat

  (** Generate TMX header information from enriched TUs. *)
  let generate_tmx_metadata tus
    ~crawled_site ~source_language ~target_language =
    let length_in_tus = List.length tus in
    let words_in_l1 = List.fold tus ~init: [] ~f: (fun p ->
      function
        | _, ({language = s_lang; _}, s_text :: []) :: _ ->
          if s_lang = source_language  then (String.split ~on: ' ' s_text) :: p
          else p
        | _ -> failwith "Ill-formed entry") |> List.concat in
    let words_in_l2 = List.fold tus ~init: [] ~f: (fun p ->
      function
        | _, _ :: ({language = t_lang; _}, t_text :: []) :: [] ->
            if t_lang = target_language then (String.split ~on: ' ' t_text) :: p
            else p
        | _ -> failwith "Ill-formed entry") |> List.concat in
    let nb_words_in_l1 = List.length words_in_l1 in
    let nb_words_in_l2 = List.length words_in_l2 in
    let nb_uniq_words_in_l1 = String.Set.of_list words_in_l1 |> String.Set.length
    in
    let nb_uniq_words_in_l2 = String.Set.of_list words_in_l2 |> String.Set.length
    in
    let creationtool = Printf.sprintf "mALIGNa;ILSP-FC;ELDA-CMTK:%s" __MODULE__ in
    let description = "Acquisition of bilingual data (from multilingual websites), normalization, cleaning, deduplication and identification of parallel documents have been done by ILSP-FC tool. Maligna aligner was used for alignment of segments. Merging/filtering of segment pairs has also been applied. Overal management, and further quality control and validation have been done by ELDA-CMTK toolkit."
    in
    {adminlang = `En; creationdate = Time.now (); creationtool;
     crawled_site; dsi = Other;
     creationtoolversion = "2";
     datatype = "plaintext"; o_tmf = "al"; segtype = Block;
     srclang = source_language; distributor = "ELRC Project"; description;
     availability = Yes; l1 = source_language; l2 = target_language;
     length_in_tus; nb_words_in_l1; nb_words_in_l2; nb_uniq_words_in_l1;
     nb_uniq_words_in_l2}

  (** Public main module API function. Takes source and target languages, an
   * optional crawled site, and the lower and upper thresholds for the five
   * error types, the input full report with TUs, filters out the TUs, and
   * computes error-related statistics, and generates a TMX XML file.*)
  let driver ~source_language ~target_language
             ?(delimiter=';') ~input_filename ~output_filename
             ?(crawled_site=None)
             ?(th_inf_alignment_error=None)
             ?(th_sup_alignment_error=None)
             ?(th_inf_translation_error=None)
             ?(th_sup_translation_error=None)
             ?(th_inf_tokenization_error=None)
             ?(th_sup_tokenization_error=None)
             ?(th_inf_language_identification_error=None)
             ?(th_sup_language_identification_error=None)
             ?(th_inf_machine_translated_text=None)
             ?(th_sup_machine_translated_text=None)
             ?(th_inf_character_formatting_error=None)
             ?(th_sup_character_formatting_error=None)
             () =
    let data =
      extract_tus ~delimiter ~source_language ~target_language ~crawled_site
                  input_filename
      |> enrich_and_prune_tus
        ~ths_inf: [(Alignment_error, th_inf_alignment_error);
                   (Translation_error, th_inf_translation_error);
                   (Tokenization_error, th_inf_tokenization_error);
                   (Language_identification_error,
                    th_inf_language_identification_error);
                   (Machine_translated_text, th_inf_machine_translated_text);
                   (Character_formatting_error,
                    th_inf_character_formatting_error)]
        ~ths_sup: [(Alignment_error, th_sup_alignment_error);
                   (Translation_error, th_sup_translation_error);
                   (Tokenization_error, th_sup_tokenization_error);
                   (Language_identification_error,
                    th_sup_language_identification_error);
                   (Machine_translated_text, th_sup_machine_translated_text);
                   (Character_formatting_error,
                   th_sup_character_formatting_error)] in
    let tu_children =
      List.map data ~f:(fun tu ->
        let tu_attrs = attributes_from_node (Tu tu) in
        let tu_props = proptypes_from_node (Tu tu) in
        let prop_xmls =
          List.map tu_props ~f: proptype_t_to_xml
          |> List.concat in
        let _, tuvs = tu in
        let tuvs_xml = List.map tuvs ~f: (fun tuv ->
          let tuv_attrs = attributes_from_node (Tuv tuv) in
          let tuv_props =
            List.map (proptypes_from_node (Tuv tuv)) ~f: (proptype_t_to_xml)
            |> List.concat in
          let _, texts = tuv in
          element_to_xml ~element:"tuv" tuv_attrs
            ~children: (tuv_props @
                        (List.map texts ~f: (
                          fun text ->
                            element_to_xml ~element: "seg" []
                                           ~children: [Xml.PCData text]))))
        in
        element_to_xml ~element:"tu" tu_attrs ~children: (prop_xmls @ tuvs_xml))
    in
    let tmx =
      generate_tmx_metadata data ~source_language ~target_language
        ~crawled_site in
    let tmx_attrs = attributes_from_node (Tmx (tmx, [])) in
    let tmx_props =
      proptypes_from_node (Tmx (tmx, []))
      |> List.map ~f: proptype_t_to_xml
      |> List.concat in
    let whole_tmx =
      element_to_xml
        ~element: "tmx"
        ~children: [(element_to_xml ~element: "header" tmx_attrs
                                    ~children: tmx_props);
                    (element_to_xml ~element: "body" [] ~children: tu_children)]
        [] in
    Out_channel.write_all
      ~data: ({|<?xml version="1.0" encoding="UTF-8" standalone="yes"?>|} ^
              "\n" ^ Xml.to_string_fmt whole_tmx)
      output_filename
end

let command =
  Command.basic_spec
    ~summary: " Automatically choose relevant TUs and build TMX files."
    ~readme: (fun () -> "=== Copyright © 2016 ELDA - All rights reserved ===\n")
    Command.Spec.(
      empty
      +> flag "-d" (optional_with_default ";" string)
        ~doc: " Delimiter for the reports CSV files (';' by default)"
      +> flag "--source-language" (required string)
        ~doc: " Source language ISO-639-1 code"
      +> flag "--target-language" (required string)
        ~doc: " Target language ISO-639-1 code"
      +> flag "--crawled-web-site" (optional string)
        ~doc: " Optional crawled web site"
      +> flag "-rf" (required string)
        ~doc: " Full report CSV file with the annotated TUs"
      +> flag "-of" (optional string)
        ~doc: " Optional output TMX file. If not specified, TMX saved in \
               output_data_source_language-target_language_crawled_site.tmx"
      +> flag "--lower-threshold-alignment-error" (optional float)
        ~doc: " Lower threshold for the alignment error"
      +> flag "--upper-threshold-alignment-error" (optional float)
        ~doc: " Upper threshold for the alignment error"
      +> flag "--lower-threshold-tokenization-error" (optional float)
        ~doc: " Lower threshold for the tokenization error"
      +> flag "--upper-threshold-tokenization-error" (optional float)
        ~doc: " Upper threshold for the tokenization error"
      +> flag "--lower-threshold-translation-error" (optional float)
        ~doc: " Lower threshold for the translation error"
      +> flag "--upper-threshold-translation-error" (optional float)
        ~doc: " Upper threshold for the translation error"
      +> flag "--lower-threshold-language-identification-error" (optional float)
        ~doc: " Lower threshold for the language identification error"
      +> flag "--upper-threshold-language-identification-error" (optional float)
        ~doc: " Upper threshold for the language identification error"
      +> flag "--lower-threshold-machine-translation" (optional float)
        ~doc: " Lower threshold for the machine translations"
      +> flag "--upper-threshold-machine-translation" (optional float)
        ~doc: " Upper threshold for the machine translations"
      +> flag "--lower-threshold-character-formatting-error" (optional float)
        ~doc: " Lower threshold for the character formatting errors"
      +> flag "--upper-threshold-character-formatting-error" (optional float)
        ~doc: " Upper threshold for the character formatting errors"
    )
    (fun delimiter' source_language' target_language' crawled_site
         input_filename
         output_filename' th_inf_alignment_error th_sup_alignment_error
         th_inf_tokenization_error th_sup_tokenization_error
         th_inf_translation_error th_sup_translation_error
         th_inf_language_identification_error
         th_sup_language_identification_error
         th_inf_machine_translated_text th_sup_machine_translated_text
         th_inf_character_formatting_error th_sup_character_formatting_error ->
      let delimiter = Char.of_string delimiter' in
      let output_filename =
        match output_filename' with
        | None ->
            let site =
              match crawled_site with
              | None -> ""
              | Some site' -> "_" ^ site'
            in
            Printf.sprintf
              "output_data_%s-%s%s.tmx" source_language' target_language' site
        | Some fname -> fname in
      let source_language, target_language =
        let language_t_of_string lang =
          String.capitalize lang |> Sexp.of_string
          |> Crawler_tmxbuilder.language_t_of_sexp in
        language_t_of_string source_language',
        language_t_of_string target_language'
      in
      Crawler_tmxbuilder.driver
        ~source_language ~target_language ~delimiter ~input_filename
        ~output_filename ~crawled_site ~th_inf_alignment_error
        ~th_sup_alignment_error ~th_inf_translation_error
        ~th_sup_translation_error ~th_inf_tokenization_error
        ~th_sup_tokenization_error ~th_inf_language_identification_error
        ~th_sup_language_identification_error
        ~th_inf_machine_translated_text ~th_sup_machine_translated_text
        ~th_inf_character_formatting_error ~th_sup_character_formatting_error
      )

let () = Command.run ~version: "1.1.1" ~build_info: "ELDA on Debian" command
