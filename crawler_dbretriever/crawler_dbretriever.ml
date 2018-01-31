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

(** The goal of this tool is to enable the users to retrieve data from
    a PostgreSQL database.
    The tool will allow the users to:

    - perform custom SQL queries via a REPL
    - dump the results to CSV, with the appropriate header and format.
    - fully dump the database with no custom SQL query.

    The starting point of this tool is the Postgresql-based mini-repl, available
    from:
    https://github.com/mmottl/postgresql-ocaml/blob/master/examples/prompt.ml*)

open Core

module CrawlerDbretriever : sig

  val last_res: Postgresql.result option ref

  val print_res: keep_ids: bool ->
                 <copy_out: (string -> unit) -> unit;
                  reset: unit; ..> ->
    <cmd_status: string; error: string; get_all_lst: string list list;
     get_fnames_lst: string list; nfields: int; ntuples: int;
     status: Postgresql.result_status; ..> -> unit

  val serialize_res: out_file_name: string -> keep_ids: bool ->
    ?delimiter: char ->
    <get_all_lst: string list list; get_fnames_lst: string list;
     status: Postgresql.result_status; ..> -> unit

  val dump_res: keep_ids: bool -> ?print: bool ->
                <copy_out: (string -> unit) -> unit;
                 get_result: Postgresql.result option;
                 reset: unit; ..> -> unit

  val driver: dbname: string -> user: string -> password: string ->
    keep_ids: bool -> ?delimiter: char -> unit -> unit

end = struct

  let name_from_field field =
    let fields_names_map =
      String.Map.of_alist_exn @@
      List.zip_exn
        ["alignment_score"; "average_alignment_score_mean";
         "average_alignment_score_variance"; "average_length_ratio_mean";
         "average_length_ratio_variance"; "crawled_web_site";
         "further_information"; "languages"; "number_of_provenances";
         "number_of_sites"; "original_web_site"; "provenance";
         "source_language"; "source_language_token_counts"; "source_text";
         "source_token_count"; "target_language";
         "target_language_token_counts"; "target_text"; "target_token_count";
         "third_party_sites_counts"; "third_party_sites_ratio";
         "token_counts"; "tu_alignment_score_mean";
         "tu_alignment_score_variance"; "tu_count"; "tu_counts";
         "tu_length_ratio_mean"; "tu_length_ratio_variance"; "web_site"]
        ["Alignment score"; "Average alignment score mean";
         "Average alignment score variance"; "Average length ratio mean";
         "Average length ratio variance"; "Crawled web site";
         "Further information"; "Languages";
         "Number of provenances"; "Number of sites"; "Original web site";
         "Provenance"; "Source language"; "Source language token counts";
         "Source text"; "Source token count"; "Target language";
         "Target language token counts"; "Target text"; "Target token count";
         "Number of distinct third-party sites"; "Third-party sites TU ratio";
         "Token counts"; "TU alignment score mean";
         "TU alignment score variance"; "TU count"; "TU counts";
         "TU length ratio mean"; "TU length ratio variance"; "Web site"] in
    try
      String.Map.find_exn fields_names_map field
    with
    | Not_found -> field

  let print_conn_info conn =
    Printf.printf "db      = %s\n" conn#db;
    Printf.printf "user    = %s\n" conn#user;
    Printf.printf "pass    = %s\n" conn#pass;
    Printf.printf "host    = %s\n" conn#host;
    Printf.printf "port    = %s\n" conn#port;
    Printf.printf "tty     = %s\n" conn#tty;
    Printf.printf "option  = %s\n" conn#options;
    Printf.printf "pid     = %i\n" conn#backend_pid

  let last_res = ref None

  let print_res ~keep_ids conn res =
    let open Postgresql in
    match res#status with
    | Empty_query -> Printf.printf "Empty query\n"
    | Command_ok -> Printf.printf "Command ok [%s]\n" res#cmd_status
    | Tuples_ok ->
      let sexp_of_reslist = [%sexp_of: string list] in
      begin 
        Printf.printf "Tuples ok\n";
        Printf.printf "%i entries with %i fields each\n" 
          res#ntuples
          (if keep_ids = true then res#nfields else res#nfields - 1);
        Printf.printf "%s\n" @@
        String.concat (if keep_ids = true then res#get_fnames_lst
                       else List.tl_exn res#get_fnames_lst) ~sep: " ";
        sexp_of_reslist @@
        List.map (if keep_ids = true then res#get_fnames_lst
                  else List.tl_exn res#get_fnames_lst) ~f: name_from_field
        |> Sexp.to_string_hum
        |> Printf.printf "%s\n";
        List.iter res#get_all_lst ~f: (
          fun el -> sexp_of_reslist (if keep_ids = true then el
                                     else List.tl_exn el)
                    |> Sexp.to_string_hum
                    |> Printf.printf "%s\n")
      end
    | Copy_out ->
      Printf.printf "Copy out:\n"; conn#copy_out (Printf.printf "%s\n")
    | Copy_in -> Printf.printf "Copy in, not handled!\n"; exit 1
    | Bad_response -> Printf.printf "Bad response: %s\n" res#error; conn#reset
    | Nonfatal_error -> Printf.printf "Non fatal error: %s\n" res#error
    | Fatal_error -> Printf.printf "Fatal error: %s\n" res#error
    | Copy_both -> Printf.printf "Copy in/out, not handled!\n"; exit 1
    | Single_tuple -> Printf.printf "Single tuple, not handled!\n"; exit 1

  let serialize_res ~out_file_name ~keep_ids ?(delimiter=';') res =
    let open Postgresql in
    match res#status with
    | Tuples_ok ->
      let header = List.map res#get_fnames_lst ~f: name_from_field in
      let data' = header :: res#get_all_lst in
      let data =
        if keep_ids = true then data' else List.map data' ~f: List.tl_exn in
      Csv.save ~separator: delimiter out_file_name data
    | _ -> Printf.eprintf "%s\n" "Cannot serialize results to CSV"

  let rec dump_res ~keep_ids ?(print=true) conn =
    match conn#get_result with
    | Some res ->
      begin
        last_res := Some res;
        if print = true then
          begin
            print_res ~keep_ids conn res;
            Out_channel.(flush stdout);
          end;
        dump_res ~keep_ids ~print conn
      end
    | None -> ()

  let rec dump_notification conn =
    let open Postgresql in
    match conn#notifies with
    | Some {Notification.name; pid; extra} ->
      Printf.printf "Notication from backend %i: [%s] [%s]\n" pid name extra;
      Out_channel.(flush stdout);
      dump_notification conn
    | None -> ()

  let listener conn =
    try
      while true do
        let socket = Unix.File_descr.of_int conn#socket in
        let _ = Unix.select ~read: [socket] ~write: [] ~except: []
            ~restart: true
            ~timeout: (`After (Time_ns.Span.of_string "1s")) ()
        in
        conn#consume_input;
        dump_notification conn
      done
    with
    | Postgresql.Error e -> Printf.eprintf "%s\n" (Postgresql.string_of_error e)
    | e -> Printf.eprintf "%s\n" (Exn.to_string e)

  type verb =
    | DUMP
    | Unknown [@@deriving sexp]

  type prep =
    | TO
    | OUTOF
    | Unknown [@@deriving sexp]

  type punct =
    | Semicolon
    | Incorrect

  let dump_parameters_from_query query =
    let atoms =
      Pcre.split ~rex: (Pcre.regexp {|\s+|([;])|}) query
      |> List.filter ~f: ((<>) "") in
    match atoms with
    | [verb; prep; out; punct] ->
      let verb' =
        match
          Pcre.extract
            ~rex: (sexp_of_verb DUMP
                   |> Sexp.to_string
                   |> Pcre.regexp ~flags: [`CASELESS])
            verb
        with
        | exception Not_found -> Unknown
        | _ -> DUMP in
      let prep' =
        match
          Pcre.extract
            ~rex: (sexp_of_prep TO
                   |> Sexp.to_string
                   |> Pcre.regexp ~flags: [`CASELESS])
            prep
        with
        | exception Not_found -> Unknown
        | _ -> TO in
      let punct' =
        match Pcre.extract ~rex: (Pcre.regexp {|^[;]$|}) punct with
        | exception Not_found -> Incorrect
        | _ -> Semicolon in
      verb', prep', out, Unknown, "", punct'
    | verb :: prep_to :: out :: prep_from :: query_punct ->
      let verb' =
        match
          Pcre.extract
            ~rex: (sexp_of_verb DUMP
                   |> Sexp.to_string
                   |> Pcre.regexp ~flags: [`CASELESS])
            verb
        with
        | exception Not_found -> Unknown
        | _ -> DUMP in
      let prep_to' =
        match
          Pcre.extract
            ~rex: (sexp_of_prep TO
                   |> Sexp.to_string
                   |> Pcre.regexp ~flags: [`CASELESS])
            prep_to
        with
        | exception Not_found -> Unknown
        | _ -> TO in
      let prep_from' =
        match
          Pcre.extract
            ~rex: (sexp_of_prep OUTOF
                   |> Sexp.to_string
                   |> Pcre.regexp ~flags: [`CASELESS])
            prep_from
        with
        | exception Not_found -> Unknown
        | _ -> OUTOF in
      begin
        match List.split_n query_punct (List.length query_punct - 1) with
        | query_atoms, [punct] ->
          let punct' =
            match Pcre.extract ~rex: (Pcre.regexp {|^[;]$|}) punct with
            | exception Not_found -> Incorrect
            | _ -> Semicolon
            in
          let query = String.concat ~sep: " " query_atoms in
          verb', prep_to', out, prep_from', query, punct'
        | _ -> failwith "Ill-formed data"
      end
    | _ -> Unknown, Unknown, query, Unknown, "", Incorrect

  let driver ~dbname ~user ~password ~keep_ids ?(delimiter=';') () =
    if Obj.is_block (Obj.repr Unix.stdin) then
      failwith "cannot run on Windows";
    let conn = new Postgresql.connection ~dbname ~user ~password () in
    print_conn_info conn;
    Out_channel.(flush stdout);
    conn#set_notice_processor 
      (fun s -> Printf.eprintf "postgresql error [%s]\n" s);
    let _ = Thread.create listener conn in
    try
      while true do
        Printf.printf "%s" "crawler_dbretriever > ";
        let s =
          begin
            Out_channel.(flush stdout);
            In_channel.(input_line_exn stdin)
          end in
        match dump_parameters_from_query s with
        | DUMP, TO, out_file_name, Unknown, "", Semicolon ->
          begin
            match !last_res with
            | None -> ()
            | Some res' -> 
              serialize_res ~delimiter ~keep_ids ~out_file_name: out_file_name
                            res';
              last_res := None
          end
        | DUMP, TO, out_file_name, OUTOF, query, Semicolon ->
            begin
              conn#send_query query;
              dump_res ~keep_ids ~print: false conn;
              match !last_res with
              | None -> ()
              | Some res' ->
                serialize_res ~delimiter ~keep_ids ~out_file_name: out_file_name
                              res';
                last_res := None
            end
        | _ ->
          conn#send_query s;
          dump_res ~keep_ids conn
      done
    with End_of_file -> conn#finish
end

let command =
  Command.basic_spec
    ~summary: "Interact with Postgresql database and dump queried data into CSV"
    ~readme: (fun () -> "=== Copyright © 2016 ELDA - All rights reserved ===\n")
    Command.Spec.(
      empty
      +> flag "--dbname" (required string)
        ~doc: " Name of the Postgresql database to connect to"
      +> flag "--username" (required string)
        ~doc: " Name of the user account to connect to the the Postgresql \
               database"
      +> flag "--password" (required string)
        ~doc: " Password of the Postgresql account"
      +> flag "-d" (optional_with_default ";" string)
        ~doc: " Delimiter to be used in the CSV file dumps"
      +> flag "--keep-ids" (optional_with_default false bool)
        ~doc: " Keep the '__id__' column (false by default)"
    )
    (fun dbname user password delimiter' keep_ids ->
       let delimiter = Char.of_string delimiter' in
       (fun () ->
          try
            CrawlerDbretriever.driver ~dbname ~user ~password ~delimiter
                                      ~keep_ids ()
          with
          | Postgresql.Error e ->
            Printf.eprintf "%s\n" (Postgresql.string_of_error e)
          | e -> Printf.eprintf "%s\n" (Exn.to_string e)
       ))

let () = Command.run ~version: "1.0" ~build_info: "ELDA on Debian" command
