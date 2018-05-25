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

(** Dedicated tool to dump CSV reports to SQLite databases.

    The idea is to define four data types, for each of the four reports:

    - full
    - synthesis
    - per-site aggregation
    - per-language pair aggregation.
    Then, the idea is to use a functor for the general operations, defining
    a generic type t for each of these four types.
*)

(** Module to store common types and accessors.*)
module Data_common = struct

  exception Dbdumper_error of string

  (**Basic common types.*)
  type eea_language_t =
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

  and eea_country_t =
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
    | Norway

  and extension_t =
    | Eu
    | Org
    | Info
    | Com
    | Int
    | Net
    | Biz

  and provenance_t =
    | Country of eea_country_t
    | Unknown of extension_t with orm

  (** EEA language accessor.*)
  let eea_language_from_string =
    function
    | "bg" -> Bg
    | "hr" -> Hr
    | "cs" -> Cs
    | "da" -> Da
    | "nl" -> Nl
    | "en" -> En
    | "et" -> Et
    | "fi" -> Fi
    | "fr" -> Fr
    | "de" -> De
    | "el" -> El
    | "hu" -> Hu
    | "ga" -> Ga
    | "it" -> It
    | "lv" -> Lv
    | "lt" -> Lt
    | "mt" -> Mt
    | "pl" -> Pl
    | "pt" -> Pt
    | "ro" -> Ro
    | "sk" -> Sk
    | "sl" -> Sl
    | "es" -> Es
    | "sv" -> Sv
    | "no" -> No
    | alien_language ->
      let fname, lnum, cnum, _ = __POS__ in
      raise (Match_failure ("Unknown language " ^ alien_language ^ " in " ^
                            fname, lnum, cnum))
  (** Provenance accessor.*)
  let provenance_from_string =
    function
    | "Austria" -> Country Austria
    | "Bulgaria" -> Country Bulgaria
    | "Belgium" -> Country Belgium
    | "Croatia" -> Country Croatia
    | "Cyprus" -> Country Cyprus
    | "Czech_Republic" -> Country Czech_Republic
    | "Denmark" -> Country Denmark
    | "Netherlands" -> Country Netherlands
    | "United_Kingdom" -> Country United_Kingdom
    | "Estonia" -> Country Estonia
    | "Finland" -> Country Finland
    | "France" -> Country France
    | "Germany" -> Country Germany
    | "Greece" -> Country Greece
    | "Hungary" -> Country Hungary
    | "Iceland" -> Country Iceland
    | "Ireland" -> Country Ireland
    | "Italy" -> Country Italy
    | "Latvia" -> Country Latvia
    | "Lithuania" -> Country Lithuania
    | "Luxembourg" -> Country Luxembourg
    | "Malta" -> Country Malta
    | "Poland" -> Country Poland
    | "Portugal" -> Country Portugal
    | "Romania" -> Country Romania
    | "Slovakia" -> Country Slovakia
    | "Slovenia" -> Country Slovenia
    | "Spain" -> Country Spain
    | "Sweden" -> Country Sweden
    | "Norway" -> Country Norway
    | alien_entry ->
      let extension_str = Str.split (Str.regexp "[:() ]") alien_entry
                          |> Core.List.last_exn in
      let extension =
        begin
          match extension_str with
          | "eu" -> Eu
          | "org" -> Org
          | "info" -> Info
          | "com" -> Com
          | "int" -> Int
          | "net" -> Net
          | "biz" -> Biz
          | ext -> 
            let fname, lnum, cnum, _ = __POS__ in
            raise (Match_failure ("Unknown provenance" ^ ext ^ " in " ^
                                  fname, lnum, cnum))
        end in
      Unknown extension

  (** Helper to convert Float.-nan to 0.*)
  let un_nan num = if Core.Float.(compare num nan) = 0 then 0. else num
end

(** Generic report type.*)
module type Data_t = sig
  type t
  val t_init: string -> (t, [ `RW ]) Orm.Db.t
  val t_save: db: (t, [ `RW ]) Orm.Db.t -> t -> unit
  val t_init_psql: db: Postgresql.connection -> unit
  val t_save_psql: db: Postgresql.connection -> data: t list -> unit
  val t_get_psql: db: Postgresql.connection -> data: t list -> t list
  val t_convert: string Core.String.Map.t -> t
  val t_lazy_get: ?custom: (t -> bool) -> (t, [< `RO | `RW]) Orm.Db.t -> unit ->
    (unit -> t option)
end

(** Synthesis report module instantiation.*)
module Data_report_synthesis = struct
  include Data_common
  type t = {crawled_web_site: string; 
            provenance: provenance_t;
            source_language: eea_language_t;
            target_language: eea_language_t;
            source_token_count: int;
            target_token_count: int;
            tu_count: int;
            tu_alignment_score_mean: float;
            tu_alignment_score_variance: float;
            tu_length_ratio_mean: float;
            tu_length_ratio_variance: float} with orm

  (** Initialize psql table.*)
  let t_init_psql ~db =
    raise (Dbdumper_error "Cannot perform \"deep\" dumps to PostgreSQL \
                           directly. Please dump to SQLite in this case.")

  (** Save data to psql table.*)
  let t_save_psql ~db ~data =
    raise (Dbdumper_error "Cannot perform \"deep\" dumps to PostgreSQL \
                           directly. Please dump to SQLite in this case.")
  (** Get data items from psql table.*)
  let t_get_psql ~db ~data =
    raise (Dbdumper_error "Cannot perform \"deep\" dumps to PostgreSQL \
                           directly. Please dump to SQLite in this case.")

  (** Convert from-CSV entry to synthesis report type.*)
  let t_convert entry =
    let open Core in
    {crawled_web_site = String.Map.find_exn entry "Crawled web site";
     provenance =
       String.Map.find_exn entry "Provenance"
       |> provenance_from_string;
     source_language =
       String.Map.find_exn entry "Source language"
       |> eea_language_from_string;
     target_language =
       String.Map.find_exn entry "Target language"
       |> eea_language_from_string;
     source_token_count =
       String.Map.find_exn entry "Source token count"
       |> Int.of_string;
     target_token_count =
       String.Map.find_exn entry "Target token count"
       |> Int.of_string;
     tu_count =
       String.Map.find_exn entry "TU count"
       |> Int.of_string;
     tu_alignment_score_mean =
       String.Map.find_exn entry "TU alignment score mean"
       |> Float.of_string;
     tu_alignment_score_variance =
       String.Map.find_exn entry "TU alignment score variance"
       |> Float.of_string;
     tu_length_ratio_mean =
       String.Map.find_exn entry "TU length ratio mean"
       |> Float.of_string;
     tu_length_ratio_variance =
       String.Map.find_exn entry "TU length ratio variance"
       |> Float.of_string}
  let t_lazy_get ?(custom=(fun _ -> true)) db_handler () =
    t_lazy_get ~custom db_handler

end

(** Shallow synthesis report module instantiation.*)
module Data_report_synthesis_shallow = struct
  include Data_common
  type t = {crawled_web_site: string;
            provenance: string;
            source_language: string;
            target_language: string;
            source_token_count: int;
            target_token_count: int;
            tu_count: int;
            tu_alignment_score_mean: float;
            tu_alignment_score_variance: float;
            tu_length_ratio_mean: float;
            tu_length_ratio_variance: float} with orm

  (** Initialize psql table.*)
  let t_init_psql ~(db: Postgresql.connection) =
    begin
      db#reset;
      db#exec "CREATE TABLE IF NOT EXISTS t \
               (__id__ serial, \
                crawled_web_site text, \
                provenance text, \
                source_language text, \
                target_language text, \
                source_token_count integer, \
                target_token_count integer, \
                tu_count integer, \
                tu_alignment_score_mean double precision, \
                tu_alignment_score_variance double precision, \
                tu_length_ratio_mean double precision, \
                tu_length_ratio_variance double precision)" |> ignore;
      db#reset
    end

  (** Save data to psql table.*)
  let t_save_psql ~(db: Postgresql.connection) ~data =
    let open Core in
    let query_params =
      List.map data ~f: (
        function
        | {crawled_web_site; provenance; source_language; target_language;
           source_token_count; target_token_count; tu_count;
           tu_alignment_score_mean; tu_alignment_score_variance;
           tu_length_ratio_mean; tu_length_ratio_variance} ->
          let fprecision =
            List.map ~f: (fun num -> Float.to_string num
                                     |> String.split ~on: '.'
                                     |> List.last_exn |> String.length)
                     [tu_alignment_score_mean; tu_alignment_score_variance;
                      tu_length_ratio_mean; tu_length_ratio_variance]
            |> List.max_elt ~compare: Int.compare |> Option.value ~default: 6 in
          Printf.sprintf
            "('%s', '%s', '%s', '%s', %d, %d, %d, %.*f, %.*f, %.*f, %.*f)"
            crawled_web_site provenance source_language target_language
            source_token_count target_token_count tu_count
            fprecision tu_alignment_score_mean fprecision
            tu_alignment_score_variance fprecision
            tu_length_ratio_mean fprecision tu_length_ratio_variance
      )
      |> String.concat ~sep: ", " in
    let query =
      Printf.sprintf
        "INSERT INTO t (crawled_web_site, provenance, source_language, \
                        target_language, source_token_count, \
                        target_token_count, tu_count, tu_alignment_score_mean, \
                        tu_alignment_score_variance, tu_length_ratio_mean, \
                        tu_length_ratio_variance) VALUES %s" query_params in
    begin
      db#reset;
      db#exec query |> ignore;
      db#reset
    end

  (** Get data items from psql table.*)
  let t_get_psql ~(db: Postgresql.connection) ~data  =
    let open Core in
    let query_base_fmt = Printf.sprintf
      "SELECT * FROM t WHERE \
       crawled_web_site IN (%s) AND \
       provenance IN (%s) AND \
       source_language IN (%s) AND \
       target_language IN (%s)" in
    let query_params =
      let len = List.length data in
      let crawled_web_site_array = Array.create ~len "" in
      let provenance_array = Array.create ~len "" in
      let source_language_array = Array.create ~len "" in
      let target_language_array = Array.create ~len "" in
      begin
      List.iteri data ~f: (fun i ->
        function
        | {crawled_web_site; provenance; source_language; target_language; _} ->
          begin
            crawled_web_site_array.(i) <- crawled_web_site;
            provenance_array.(i) <- provenance;
            source_language_array.(i) <- source_language;
            target_language_array.(i) <- target_language
          end
      );
      List.map [crawled_web_site_array; provenance_array;
                source_language_array; target_language_array]
        ~f: (Fn.compose
               (String.concat ~sep: ",") @@
               Fn.compose (List.map ~f: (fun el -> Printf.sprintf "'%s'" el)) @@
                 Fn.compose (List.dedup_and_sort ~compare: String.compare) Array.to_list)
      end in
    match query_params with
    | [crawled_web_sites; provenances; source_languages; target_languages] ->
      let results = begin db#reset;
        db#exec (query_base_fmt crawled_web_sites provenances
          source_languages target_languages) end in
      begin db#reset; results#get_all_lst
      |> List.map ~f: (
        function
        | [__id__; crawled_web_site; provenance; source_language;
           target_language;
           source_token_count'; target_token_count'; tu_count';
           tu_alignment_score_mean'; tu_alignment_score_variance';
           tu_length_ratio_mean'; tu_length_ratio_variance'] ->
          {crawled_web_site; provenance; source_language; target_language;
           source_token_count = Int.of_string source_token_count';
           target_token_count = Int.of_string target_token_count';
           tu_count = Int.of_string tu_count';
           tu_alignment_score_mean = Float.of_string tu_alignment_score_mean';
           tu_alignment_score_variance =
             Float.of_string tu_alignment_score_variance';
           tu_length_ratio_mean = Float.of_string tu_length_ratio_mean';
           tu_length_ratio_variance =
             Float.of_string tu_length_ratio_variance'}
        | _ -> raise (Dbdumper_error "Ill-formed data")) end
    | _ -> raise (Dbdumper_error "Ill-formed data")

  (** Convert from-CSV entry to synthesis report type.*)
  let t_convert entry =
    let open Core in
    {crawled_web_site = String.Map.find_exn entry "Crawled web site";
     provenance = String.Map.find_exn entry "Provenance";
     source_language = String.Map.find_exn entry "Source language";
     target_language = String.Map.find_exn entry "Target language";
     source_token_count =
       String.Map.find_exn entry "Source token count"
       |> Int.of_string;
     target_token_count =
       String.Map.find_exn entry "Target token count"
       |> Int.of_string;
     tu_count =
       String.Map.find_exn entry "TU count"
       |> Int.of_string;
     tu_alignment_score_mean =
       String.Map.find_exn entry "TU alignment score mean"
       |> Float.of_string |> un_nan;
     tu_alignment_score_variance =
       String.Map.find_exn entry "TU alignment score variance"
       |> Float.of_string |> un_nan;
     tu_length_ratio_mean =
       String.Map.find_exn entry "TU length ratio mean"
       |> Float.of_string |> un_nan;
     tu_length_ratio_variance =
       String.Map.find_exn entry "TU length ratio variance"
       |> Float.of_string |> un_nan}

  let t_lazy_get ?(custom=(fun _ -> true)) db_handler () =
    t_lazy_get ~custom db_handler
end

(** Full report module instantiation.*)
module Data_report_full = struct
  include Data_common
  type t = {crawled_web_site: string; 
            original_web_site: string;
            provenance: provenance_t;
            source_text: string;
            source_token_count: int;
            source_language: eea_language_t;
            target_text: string;
            target_token_count: int;
            target_language: eea_language_t;
            alignment_score: float;
            further_information: string} with orm

  (** Initialize psql table.*)
  let t_init_psql ~db =
    raise (Dbdumper_error "Cannot perform \"deep\" dumps to PostgreSQL \
                           directly. Please dump to SQLite in this case.")

  (** Save data to psql table.*)
  let t_save_psql ~db ~data =
    raise (Dbdumper_error "Cannot perform \"deep\" dumps to PostgreSQL \
                           directly. Please dump to SQLite in this case.")
  (** Get data items from psql table.*)
  let t_get_psql ~db ~data =
    raise (Dbdumper_error "Cannot perform \"deep\" dumps to PostgreSQL \
                           directly. Please dump to SQLite in this case.")

  (** From-CSV to specific full report type converter.*)
  let t_convert entry =
    let open Core in
    {crawled_web_site = String.Map.find_exn entry "Crawled web site";
     original_web_site = String.Map.find_exn entry "Original web site";
     provenance =
       String.Map.find_exn entry "Provenance"
       |> provenance_from_string;
     source_text = String.Map.find_exn entry "Source text";
     source_language =
       String.Map.find_exn entry "Source language"
       |> eea_language_from_string;
     source_token_count =
       String.Map.find_exn entry "Source token count"
       |> Int.of_string;
     target_text = String.Map.find_exn entry "Target text";
     target_language =
       String.Map.find_exn entry "Target language"
       |> eea_language_from_string;
     target_token_count =
       String.Map.find_exn entry "Target token count"
       |> Int.of_string;
     alignment_score =
       String.Map.find_exn entry "Alignment score"
       |> Float.of_string |> un_nan;
     further_information = String.Map.find_exn entry "Further information"
    }
  let t_lazy_get ?(custom=(fun _ -> true)) db_handler () =
    t_lazy_get ~custom db_handler
end

(** Shallow full report module instantiation.*)
module Data_report_full_shallow = struct
  include Data_common
  type t = {crawled_web_site: string;
            original_web_site: string;
            provenance: string;
            source_text: string;
            source_token_count: int;
            source_language: string;
            target_text: string;
            target_token_count: int;
            target_language: string;
            alignment_score: float;
            further_information: string} with orm

  (** Initialize psql table.*)
  let t_init_psql ~(db: Postgresql.connection) =
    begin
      db#reset;
      db#exec "CREATE TABLE IF NOT EXISTS t \
               (__id__ serial, \
                crawled_web_site text, \
                original_web_site text, \
                provenance text, \
                source_text text, \
                source_token_count integer, \
                source_language text, \
                target_text text, \
                target_token_count integer, \
                target_language text, \
                alignment_score double precision, \
                further_information text)" |> ignore;
      db#reset
    end

  (** Save data to psql table.*)
  let t_save_psql ~(db: Postgresql.connection) ~data =
    let open Core in
    let escape_quotes = Str.global_replace (Str.regexp "'") "''" in
    let query_params =
      List.map data ~f: (
        function
        | {crawled_web_site; original_web_site; provenance; source_text;
           source_token_count; source_language; target_text; target_token_count;
           target_language; alignment_score; further_information} ->
          let fprecision =
            Float.to_string alignment_score
            |> String.split ~on: '.' |> List.last_exn |> String.length in
          Printf.sprintf
            "('%s', '%s', '%s', '%s', %d, '%s', '%s', %d, '%s', %.*f, '%s')"
            crawled_web_site original_web_site provenance
            (source_text |> escape_quotes)
            source_token_count source_language
            (target_text |> escape_quotes)
            target_token_count
            target_language fprecision alignment_score further_information
      )
      |> String.concat ~sep: ", " in
    let query =
      Printf.sprintf
        "INSERT INTO t (crawled_web_site, original_web_site, provenance, \
                        source_text, source_token_count, source_language, \
                        target_text, target_token_count, target_language, \
                        alignment_score, further_information) VALUES %s"
        query_params in
    begin
      db#reset;
      db#exec query |> ignore;
      db#reset
    end

  (** Get data items from psql table.*)
  let t_get_psql ~(db: Postgresql.connection) ~data  =
    let open Core in
    let query_base_fmt = Printf.sprintf
      "SELECT * FROM t WHERE \
       crawled_web_site IN (%s) AND \
       original_web_site IN (%s) AND \
       provenance IN (%s) AND \
       source_language IN (%s) AND \
       target_language IN (%s)" in
    let query_params =
      let len = List.length data in
      let crawled_web_site_array = Array.create ~len "" in
      let original_web_site_array = Array.create ~len "" in
      let provenance_array = Array.create ~len "" in
      let source_language_array = Array.create ~len "" in
      let target_language_array = Array.create ~len "" in
      begin
      List.iteri data ~f: (fun i ->
        function
        | {crawled_web_site; original_web_site; provenance; source_language;
           target_language; _} ->
          begin
            crawled_web_site_array.(i) <- crawled_web_site;
            original_web_site_array.(i) <- original_web_site;
            provenance_array.(i) <- provenance;
            source_language_array.(i) <- source_language;
            target_language_array.(i) <- target_language
          end
      );
      List.map [crawled_web_site_array; original_web_site_array;
                provenance_array; source_language_array; target_language_array]
        ~f: (Fn.compose
               (String.concat ~sep: ",") @@
               Fn.compose (List.map ~f: (fun el -> Printf.sprintf "'%s'" el)) @@
               Fn.compose (List.dedup_and_sort ~compare: String.compare)
                          Array.to_list)
      end in
    match query_params with
    | [crawled_web_sites; original_web_sites; provenances; source_languages;
       target_languages] ->
      let results = begin db#reset;
        db#exec (query_base_fmt crawled_web_sites original_web_sites provenances
          source_languages target_languages) end in
      begin db#reset; results#get_all_lst
      |> List.map ~f: (
        function
        | [__id__; crawled_web_site; original_web_site; provenance; source_text;
           source_token_count'; source_language; target_text;
           target_token_count'; target_language; alignment_score';
           further_information] ->
          {crawled_web_site; original_web_site; provenance; source_text;
           source_language; target_text; target_language; further_information;
           source_token_count = Int.of_string source_token_count';
           target_token_count = Int.of_string target_token_count';
           alignment_score = Float.of_string alignment_score'}
        | _ -> raise (Dbdumper_error "Ill-formed data")) end
    | _ -> raise (Dbdumper_error "Ill-formed data")

  (** From-CSV to specific full report type converter.*)
  let t_convert entry =
    let open Core in
    let open Data_common in
    {crawled_web_site = String.Map.find_exn entry "Crawled web site";
     original_web_site = String.Map.find_exn entry "Original web site";
     provenance = String.Map.find_exn entry "Provenance";
     source_text = String.Map.find_exn entry "Source text";
     source_language = String.Map.find_exn entry "Source language";
     source_token_count =
       String.Map.find_exn entry "Source token count"
       |> Int.of_string;
     target_text = String.Map.find_exn entry "Target text";
     target_language = String.Map.find_exn entry "Target language";
     target_token_count =
       String.Map.find_exn entry "Target token count"
       |> Int.of_string;
     alignment_score =
       String.Map.find_exn entry "Alignment score"
       |> Float.of_string;
     further_information = String.Map.find_exn entry "Further information"
    }
  let t_lazy_get ?(custom=(fun _ -> true)) db_handler () =
    t_lazy_get ~custom db_handler
end

(** Per-site aggregation module instantiation.*)
module Data_per_site_aggregation = struct
  include Data_common
  type t = {web_site: string;
            provenance: provenance_t;
            languages: eea_language_t list;
            token_counts: int;
            tu_counts: int;
            average_alignment_score_mean: float;
            average_alignment_score_variance: float;
            average_length_ratio_mean: float;
            average_length_ratio_variance: float;
            third_party_sites_ratio: float;
            third_party_sites_counts: int} with orm

  (** Initialize psql table.*)
  let t_init_psql ~db =
    raise (Dbdumper_error "Cannot perform \"deep\" dumps to PostgreSQL \
                           directly. Please dump to SQLite in this case.")

  (** Save data to psql table.*)
  let t_save_psql ~db ~data =
    raise (Dbdumper_error "Cannot perform \"deep\" dumps to PostgreSQL \
                           directly. Please dump to SQLite in this case.")
  (** Get data items from psql table.*)
  let t_get_psql ~db ~data =
    raise (Dbdumper_error "Cannot perform \"deep\" dumps to PostgreSQL \
                           directly. Please dump to SQLite in this case.")

  (** From-CSV entry to specific per-site aggregation type converter.*)
  let t_convert entry =
    let open Core in
    {web_site = String.Map.find_exn entry "Web site";
     provenance =
       String.Map.find_exn entry "Provenance"
       |> provenance_from_string;
     languages =
       String.Map.find_exn entry "Languages"
       |> Str.split (Str.regexp " & ")
       |> List.map ~f: eea_language_from_string;
     token_counts =
       String.Map.find_exn entry "Token counts"
       |> Int.of_string;
     tu_counts =
       String.Map.find_exn entry "TU counts"
       |> Int.of_string;
     average_alignment_score_mean =
       String.Map.find_exn entry "Average alignment score mean"
       |> Float.of_string |> un_nan;
     average_alignment_score_variance =
       String.Map.find_exn entry "Average alignment score variance"
       |> Float.of_string |> un_nan;
     average_length_ratio_mean =
       String.Map.find_exn entry "Average length ratio mean"
       |> Float.of_string |> un_nan;
     average_length_ratio_variance =
       String.Map.find_exn entry "Average length ratio variance"
       |> Float.of_string |> un_nan;
     third_party_sites_ratio =
       String.Map.find_exn entry "Third-party sites TU ratio"
       |> Float.of_string |> un_nan;
     third_party_sites_counts =
       String.Map.find_exn entry "Number of distinct third-party sites"
       |> Int.of_string}

  let t_lazy_get ?(custom=(fun _ -> true)) db_handler () =
    t_lazy_get ~custom db_handler
end

(** Shallow per-site aggregation module instantiation.*)
module Data_per_site_aggregation_shallow = struct
  type t = {web_site: string;
            provenance: string;
            languages: string list;
            token_counts: int;
            tu_counts: int;
            average_alignment_score_mean: float;
            average_alignment_score_variance: float;
            average_length_ratio_mean: float;
            average_length_ratio_variance: float;
            third_party_sites_ratio: float;
            third_party_sites_counts: int} with orm

  (** Initialize psql table.*)
  let t_init_psql ~db =
    let open Data_common in
    raise (Dbdumper_error "Per-site aggregation dump to postgresql not \
                           implemented yet.")

  (** Save data to psql table.*)
  let t_save_psql ~db ~data =
    let open Data_common in
    raise (Dbdumper_error "Per-site aggregation dump to postgresql not \
                           implemented yet.")

  (** Get data items from psql table.*)
  let t_get_psql ~db ~data =
    let open Data_common in
    raise (Dbdumper_error "Per-site aggregation dump to postgresql not \
                           implemented yet.")

  (** From-CSV entry to specific per-site aggregation type converter.*)
  let t_convert entry =
    let open Core in
    let open Data_common in
    {web_site = String.Map.find_exn entry "Web site";
     provenance = String.Map.find_exn entry "Provenance";
     languages =
       String.Map.find_exn entry "Languages"
       |> Str.split (Str.regexp " & ");
     token_counts =
       String.Map.find_exn entry "Token counts"
       |> Int.of_string;
     tu_counts =
       String.Map.find_exn entry "TU counts"
       |> Int.of_string;
     average_alignment_score_mean =
       String.Map.find_exn entry "Average alignment score mean"
       |> Float.of_string |> un_nan;
     average_alignment_score_variance =
       String.Map.find_exn entry "Average alignment score variance"
       |> Float.of_string |> un_nan;
     average_length_ratio_mean =
       String.Map.find_exn entry "Average length ratio mean"
       |> Float.of_string |> un_nan;
     average_length_ratio_variance =
       String.Map.find_exn entry "Average length ratio variance"
       |> Float.of_string |> un_nan;
     third_party_sites_ratio =
       String.Map.find_exn entry "Third-party sites TU ratio"
       |> Float.of_string |> un_nan;
     third_party_sites_counts =
       String.Map.find_exn entry "Number of distinct third-party sites"
       |> Int.of_string}

  let t_lazy_get ?(custom=(fun _ -> true)) db_handler () =
    t_lazy_get ~custom db_handler
end

(** Per-language pair aggregation module instantiation.*)
module Data_per_language_pair_aggregation = struct
  include Data_common
  type t = {source_language: eea_language_t;
            target_language: eea_language_t;
            source_language_token_counts: int;
            target_language_token_counts: int;
            number_of_sites: int;
            number_of_provenances: int;
            tu_counts: int;
            average_alignment_score_mean: float;
            average_alignment_score_variance: float;
            average_length_ratio_mean: float;
            average_length_ratio_variance: float} with orm

  (** Initialize psql table.*)
  let t_init_psql ~db =
    raise (Dbdumper_error "Cannot perform \"deep\" dumps to PostgreSQL \
                           directly. Please dump to SQLite in this case.")

  (** Save data to psql table.*)
  let t_save_psql ~db ~data =
    raise (Dbdumper_error "Cannot perform \"deep\" dumps to PostgreSQL \
                           directly. Please dump to SQLite in this case.")
  (** Get data items from psql table.*)
  let t_get_psql ~db ~data =
    raise (Dbdumper_error "Cannot perform \"deep\" dumps to PostgreSQL \
                           directly. Please dump to SQLite in this case.")

  (** From-CSV entry to per-language pair aggregation type converter.*)
  let t_convert entry =
    let open Core in
    {source_language =
       String.Map.find_exn entry "Source language"
       |> eea_language_from_string;
     target_language =
       String.Map.find_exn entry "Target language"
       |> eea_language_from_string;
     source_language_token_counts =
       String.Map.find_exn entry "Source language token counts"
       |> Int.of_string;
     target_language_token_counts =
       String.Map.find_exn entry "Target language token counts"
       |> Int.of_string;
     number_of_sites =
       String.Map.find_exn entry "Number of sites"
       |> Int.of_string;
     number_of_provenances =
       String.Map.find_exn entry "Number of provenances"
       |> Int.of_string;
     tu_counts =
       String.Map.find_exn entry "TU counts"
       |> Int.of_string;
     average_alignment_score_mean =
       String.Map.find_exn entry "Average alignment score mean"
       |> Float.of_string;
     average_alignment_score_variance =
       String.Map.find_exn entry "Average alignment score variance"
       |> Float.of_string |> un_nan;
     average_length_ratio_mean =
       String.Map.find_exn entry "Average length ratio mean"
       |> Float.of_string |> un_nan;
     average_length_ratio_variance =
       String.Map.find_exn entry "Average length ratio variance"
       |> Float.of_string |> un_nan
    }

  let t_lazy_get ?(custom=(fun _ -> true)) db_handler () =
    t_lazy_get ~custom db_handler
end

(** Shallow per-language pair aggregation module instantiation.*)
module Data_per_language_pair_aggregation_shallow = struct
  type t = {source_language: string;
            target_language: string;
            source_language_token_counts: int;
            target_language_token_counts: int;
            number_of_sites: int;
            number_of_provenances: int;
            tu_counts: int;
            average_alignment_score_mean: float;
            average_alignment_score_variance: float;
            average_length_ratio_mean: float;
            average_length_ratio_variance: float} with orm

  (** Initialize psql table.*)
  let t_init_psql ~db =
    let open Data_common in
    raise (Dbdumper_error "Per-language pair aggregation dump to postgresql \
                           not implemented yet.")

  (** Save data to psql table.*)
  let t_save_psql ~db ~data =
    let open Data_common in
    raise (Dbdumper_error "Per-language pair aggregation dump to postgresql \
                           not implemented yet.")

  (** Get data items from psql table.*)
  let t_get_psql ~db ~data =
    let open Data_common in
    raise (Dbdumper_error "Per-language pair aggregation dump to postgresql \
                           not implemented yet.")

  (** From-CSV entry to per-language pair aggregation type converter.*)
  let t_convert entry =
    let open Core in
    let open Data_common in
    {source_language = String.Map.find_exn entry "Source language";
     target_language = String.Map.find_exn entry "Target language";
     source_language_token_counts =
       String.Map.find_exn entry "Source language token counts"
       |> Int.of_string;
     target_language_token_counts =
       String.Map.find_exn entry "Target language token counts"
       |> Int.of_string;
     number_of_sites =
       String.Map.find_exn entry "Number of sites"
       |> Int.of_string;
     number_of_provenances =
       String.Map.find_exn entry "Number of provenances"
       |> Int.of_string;
     tu_counts =
       String.Map.find_exn entry "TU counts"
       |> Int.of_string;
     average_alignment_score_mean =
       String.Map.find_exn entry "Average alignment score mean"
       |> Float.of_string |> un_nan;
     average_alignment_score_variance =
       String.Map.find_exn entry "Average alignment score variance"
       |> Float.of_string |> un_nan;
     average_length_ratio_mean =
       String.Map.find_exn entry "Average length ratio mean"
       |> Float.of_string |> un_nan;
     average_length_ratio_variance =
       String.Map.find_exn entry "Average length ratio variance"
       |> Float.of_string |> un_nan
    }
  let t_lazy_get ?(custom=(fun _ -> true)) db_handler () =
    t_lazy_get ~custom db_handler
end

(** Generic dumper functor type.*)
module type CrawlerDumper_t = sig
  type t
  val dump_to_db: ?warn: bool -> backend: [`Sqlite | `Postgresql] ->
    username: string -> password: string -> db_filename: string ->
    data: t list -> unit
  val to_t: input: string Core.String.Map.t list -> t list
end

(** Generic dumper functor implementation.*)
module CrawlerDumper (Data : Data_t) : CrawlerDumper_t = struct
  type t = Data.t

  (** Generic functorized DB dumper.*)
  let dump_to_db ?(warn=false) ~backend ~username ~password ~db_filename ~data =
    let open Data in
    match backend with
    | `Sqlite ->
        let db_handle = t_init db_filename in
        let values = t_lazy_get ~custom: (fun _ -> true) db_handle () in
        let i = ref 0 in
        Core.List.iter data ~f: (
          fun item ->
            begin
              if warn = true then
                let items =
                  Core.List.map (Core.List.range 0 @@ List.length data)
                    ~f: (fun _ -> lazy (values ())) in
                if Core.List.exists items ~f: (
                    fun el ->
                      let dat = Lazy.force el in
                      if dat = Some item then true else false) = true then
                        if !i = 0 then
                          begin
                            i := !i + 1;
                            Printf.printf "%s\n" "About to duplicate data. \
                                                  Continue [y/N]?";
                            let yn = read_line () in
                            match yn with
                            | "yes" | "Y" | "y" | "Yes" | "YES" ->
                              Printf.printf "%s\n" "ok"
                            | _ -> exit 1
                          end
                        else
                          Printf.printf "%s\n" "Inserting duplicate entry...";
            end;
            t_save ~db: db_handle item)
    | `Postgresql ->
      let open Core in
      let index_in alist elem =
        try
          List.find_mapi_exn alist ~f: (
            fun i el -> if el = elem then Some i else None)
        with
        | Not_found -> -1 in
      let fname_bits =
        Filename.split_extension db_filename |> fst |> Filename.basename
        |> String.split ~on: '_' in
      let offset_0 = index_in fname_bits "report" in
      let offset_1 = index_in fname_bits "per" in
      let offset_2 = index_in fname_bits "aggregation" in
      let dbname =
        if List.for_all [offset_1; offset_2] ~f: ((<>) (-1)) then
          List.slice fname_bits offset_1 (offset_2 + 1) @ ["data"]
          |> String.concat ~sep: "_"
        else if offset_0 <> -1 then
          List.slice fname_bits 1 2 @ ["data"] |> String.concat ~sep: "_"
        else
          raise (Arg.Bad "Ill-formed output database file name. Should start \
                          with 'report' and, for the aggregates, contain 'per' \
                          and 'aggregation'.")
      in
      let conn = new Postgresql.connection ~dbname ~user: username ~password ()
      in
      begin
        Printf.printf "Connected to PostgreSQL server: %s; %s; %s\n"
          dbname username password;
        t_init_psql ~db: conn;
        begin
          if warn = true then
            let results = t_get_psql ~db: conn ~data in
            if List.length results > 0 then
              begin
                Printf.printf "%s\n" "About to duplicate data. Continue [y/N]?";
                let yn =
                  begin
                    Out_channel.(flush stdout);
                    In_channel.(input_line_exn stdin)
                  end in
                match yn with
                | "yes" | "Y" | "y" | "Yes" | "YES" -> Printf.printf "%s\n" "ok"
                | _ -> exit 1
              end
        end;
        t_save_psql ~db: conn ~data
      end

  (** Generic converter helper, from CSV to list of specific report type
      entries.*)
  let to_t ~input =
    Core.List.map input ~f: (fun iel -> Data.t_convert iel)
end

(** Driver of the dumper. Main goal: automatically get the input report kind
    from the input CSV structure and perform the DB dump.*)
module CrawlerDumperDriver : sig
  type data_kind_t
  val dump_to_db: ?shallow: bool -> ?warn: bool ->
    backend: [`Sqlite | `Postgresql] -> username: string -> password: string ->
    kind: data_kind_t -> db_filename: string ->
    data: string Core.String.Map.t list -> unit -> unit
  val get_kind_from_data: data: string Core.String.Map.t list -> data_kind_t
  val load_data_from_file: filename: string -> delimiter: char ->
    string Core.String.Map.t list

end = struct

  (** Type of the input report kind.*)
  type data_kind_t =
    | Report_full
    | Report_synthesis
    | Per_site_aggregation
    | Per_language_pair_aggregation

  (** Main DB dumper driver. This is where the heavy lifting takes place: first,
      automatically load the data and determine the report type, then properly
      instantiate the generic dumper functor, then perform the actual dump.*)
  let dump_to_db
      ?(shallow=false) ?(warn=false) ~backend ~username ~password ~kind
      ~db_filename ~data () =
    let dumper =
      match shallow, kind with
      | false, Report_full ->
        (module CrawlerDumper (Data_report_full) : CrawlerDumper_t)
      | false, Report_synthesis ->
        (module CrawlerDumper(Data_report_synthesis) : CrawlerDumper_t)
      | false, Per_site_aggregation ->
        (module CrawlerDumper(Data_per_site_aggregation) : CrawlerDumper_t)
      | false, Per_language_pair_aggregation ->
        (module CrawlerDumper(Data_per_language_pair_aggregation): CrawlerDumper_t)
      | true, Report_full ->
        (module CrawlerDumper (Data_report_full_shallow) : CrawlerDumper_t)
      | true, Report_synthesis ->
        (module CrawlerDumper(Data_report_synthesis_shallow) : CrawlerDumper_t)
      | true, Per_site_aggregation ->
        (module CrawlerDumper(Data_per_site_aggregation_shallow) : CrawlerDumper_t)
      | true, Per_language_pair_aggregation ->
        (module CrawlerDumper(Data_per_language_pair_aggregation_shallow) : CrawlerDumper_t)
    in
    let module M = (val dumper : CrawlerDumper_t) in
    let open M in
    let data' = to_t data in
    dump_to_db ~username ~password ~warn ~backend ~db_filename ~data: data'

  (** Helper to load mapped data from CSV file*)
  let load_data_from_file ~filename ~delimiter =
    let open Core in
    let data = Csv.load filename ~separator: delimiter in
    match data with
    | hd :: tl ->
      List.map tl ~f: (
        fun datum -> List.map2_exn hd datum ~f: (
            fun hdel datel -> hdel, datel))
      |> List.map ~f: String.Map.of_alist_exn
    | [] -> []

  (** Helper to infer data kind from data.*)
  let get_kind_from_data ~data =
    let open Core in
    match data with
    | first :: _ -> 
      let keyset = String.Map.keys first |> String.Set.of_list in
      let report_full_keyset =
        String.Set.of_list 
          ["Crawled web site"; "Original web site"; "Provenance";
           "Source text"; "Source token count"; "Source language";
           "Target text";
           "Target token count"; "Target language"; "Alignment score";
           "Further information"] in
      let report_synthesis_keyset =
        String.Set.of_list
          ["Crawled web site"; "Provenance"; "Source language";
           "Target language"; "Source token count"; "Target token count";
           "TU count"; "TU alignment score mean"; "TU alignment score variance";
           "TU length ratio mean"; "TU length ratio variance"] in
      let per_site_aggreg_keyset =
        String.Set.of_list
          ["Web site"; "Provenance"; "Languages"; "Token counts"; "TU counts";
           "Average alignment score mean"; "Average alignment score variance";
           "Average length ratio mean"; "Average length ratio variance";
           "Third-party sites TU ratio";
           "Number of distinct third-party sites"] in
      let per_language_pair_aggreg_keyset =
        String.Set.of_list
          ["Source language"; "Target language"; "Source language token counts";
           "Target language token counts"; "Number of sites";
           "Number of provenances"; "TU counts"; "Average alignment score mean";
           "Average alignment score variance"; "Average length ratio mean";
           "Average length ratio variance"] in
      if String.Set.compare keyset report_full_keyset = 0 then
        Report_full
      else if String.Set.compare keyset report_synthesis_keyset = 0 then
        Report_synthesis
      else if String.Set.compare keyset per_site_aggreg_keyset = 0 then
        Per_site_aggregation
      else if String.Set.compare keyset per_language_pair_aggreg_keyset = 0 then
        Per_language_pair_aggregation
      else
        failwith "Cannot get kind from the data presented"
    | _ -> failwith "Cannot get kind from no data"

end

let main () =
  let open Core in
  let open CrawlerDumperDriver in
  let usage_msg =
    String.concat
      ~sep: "\n"
      ["Automatically dump CSV reporting data to SQLite or PostgreSQL databases;";
       "=== Copyright © ELDA 2016 - All rights reserved ===\n"] in
  if Sys.argv |> Array.length < 2 then
    let errmsg = usage_msg ^ "\n" ^
                 "No argument supplied. Please supply -help or --help to see \
                  the arguments\n" in
    Printf.printf "%s" errmsg
  else
    let delimiter = ref ";" in
    let input_file_name = ref "" in
    let output_file = ref "" in
    let deep = ref false in
    let warn_on_duplicates = ref false in
    let backend = ref `Postgresql in
    let psql_uname = ref (Unix.getlogin ()) in
    let psql_passwd = ref "" in
    let speclist = [
      ("-d", Arg.Set_string delimiter, "Delimiter of the CSV input files");
      ("-i", Arg.Set_string input_file_name, "Name of the input CSV file");
      ("-o", Arg.Set_string output_file, "Name of the database (PostgreSQL \
                                          address or SQLite file)");
      ("--deep", Arg.Set deep, "Dump data in deep mode (false by default)");
      ("--warn", Arg.Set warn_on_duplicates, "Warn if we are about to insert \
                                              duplicate data");
      ("--backend", Arg.Symbol (["sqlite"; "postgresql"],
                                (function
                                  | "sqlite" -> backend := `Sqlite
                                  | _ -> ())),
       "Database backend type (postgresql by default)");
      ("--username", Arg.Set_string psql_uname, "PostgreSQL user name (current \
                                                 login by default)");
      ("--password", Arg.Set_string psql_passwd,
       "Postgresql password (required when the PostgreSQL backend is used)")
    ] in
    Arg.parse speclist (fun x -> raise (Arg.Bad ("Bad argument: " ^ x)))
      usage_msg;
    if !input_file_name = "" then
      raise (Arg.Bad "Please supply an input file name");
    let output_file_name =
      if !output_file = "" then
        let fname_base , _ = Filename.split_extension !input_file_name in
        fname_base ^ ".sqlite"
      else !output_file in
    let data = load_data_from_file
        ~filename: !input_file_name
        ~delimiter: (!delimiter |> Char.of_string) in
    let kind = get_kind_from_data data in
    dump_to_db
      ~username: !psql_uname
      ~password: !psql_passwd
      ~shallow: (not !deep)
      ~backend: !backend
      ~warn: !warn_on_duplicates ~data ~kind ~db_filename: output_file_name ()

let () = main ()

