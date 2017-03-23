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

(** The goal of this tool is to pretty print reports in the CSV format.
 *
 * Algorithm: if the user chooses to,
 * then align and print all columns on the same line, separated by a computed
 * number of spaces.
 *
 * If the user chooses so, then print each column on its separate line, and
 * separate rows with a blank line.
 *
 * Also, in both cases, allow the user to select the columns (by numbers or by
 * letters like in Excel) to be printed, and the rows to be printed (by numbers).
 * 
 * These selections can be either idividual, like index_1,index_2, by spans,
 * like index_1-index_k, or combinations of the two, where index is the row or
 * column index / number.
 *
 * *)
open Core.Std

module CrawlerPprinter : sig

  val ids_from_data: string list list -> string list

  val pretty_print_data: ?seplines: bool -> ?fieldsep: char ->
    linesep: string -> ids: string list -> concatenate_first_lines: int ->
    string list list -> string list

  val pprint_driver: ?row_selections: string option ->
    ?col_selections: string option -> ?separator: char -> seplines: bool ->
    ?linesep: string -> ?concatenate_first_lines: int ->
    input_file_name: string -> unit -> string list

end = struct

  (** Helper to generate indexes from numeric or character selections.*)
  let indexes_from_selection selection =
    Str.split (Str.regexp {|[ ]*[,;]+[ ]*|}) selection
    |> List.map ~f: (fun span ->
        let chunks = Str.split (Str.regexp {|[ ]*[-_]+[ ]*|}) span in
        match chunks with
        | [start; stop] ->
          let offsets =
            begin
              try
                List.map chunks ~f: Int.of_string
              with
              | Failure _ ->
                try
                  List.map chunks ~f: (
                    fun chunk -> Char.of_string chunk |> Char.uppercase 
                                 |> Char.to_int |> (-) 64 |> abs)
                with
                | Failure _ -> failwith "Please provide a range of numbers or \
                                         characters in the a-z, or A-Z range"
            end
            |> List.sort ~cmp: compare
          in
          List.range ~start: `inclusive ~stop: `inclusive 
            (List.hd_exn offsets) (List.last_exn offsets)
        | [index] when index <> "" ->
          begin
            try
              [Int.of_string index]
            with
            | Failure _ ->
              try
                [Char.of_string index |> Char.uppercase |> Char.to_int 
                 |> (-) 64 |> abs]
              with
              | Failure _ -> failwith "Please provide a number or a character \
                                       from a to z or from A to Z"
          end
        | [index] -> []
        | _ -> failwith "Ill-formed selection. Please provide your selection as \
                         index_1-index_i,index_j and variations on these choices."
      )
    |> List.concat

  (** Helper to retain a specified set of indexes from CSV-retrieved data.
   *  Algorithm: prune the list list structure on the ~rows and ~cols indexes
   *  lists.*)
  let prune_data ?(rows=None) ?(cols=None) data =
    (* Sort in_list according to the order of the indexes in index_list. If
     * index_list is shorter than in_list, then only indexes present in
     * index_list will be taken into account. Hence, the function sorts and prunes
     * at the same time.*)
    let sorti in_list index_list =
      let rec out_list in_list index_list acc =
        match index_list with
        | hd_index :: tl_index when hd_index < List.length in_list && 
                                    hd_index >= 0 ->
          let out_hd = List.nth_exn in_list hd_index in
          out_list in_list tl_index (out_hd :: acc)
        | _ -> acc in
      let index_list' = List.map index_list ~f: (fun i -> i - 1) in
      out_list in_list index_list' [] |> List.rev in
    match rows, cols with
    | Some rows', Some cols' ->
      List.filter_mapi data ~f: (fun i datum -> 
          if List.mem rows' (i + 1) then Some (sorti datum cols') else None)
    | Some rows', None -> List.filteri data ~f: (
        fun i _ -> List.mem rows' (i + 1))
    | None, Some cols' -> List.map data ~f: (fun dat -> sorti dat cols')
    | None, None -> data


  (** Function to pretty print data on separate lines, with each data item unique
   * ID included.*)
  let pretty_print_linear ~ids ~linesep ?(concatenate_first_lines=1) data =
    List.map2_exn ids data ~f: (fun id dat -> id :: List.append dat [linesep])
    |> List.map ~f: (
      fun dat -> 
        let to_concat, rest = List.split_n dat concatenate_first_lines in
        ("[" ^ (String.concat ~sep: "; " to_concat) ^ "]") :: rest)
    |> List.concat

  (** Function to pretty print data in the CSV format.*)
  let pretty_print_csv ?(header=None) ?(separator=';') data =
    let data' = match header with
    | None -> data
    | Some hd' -> hd' :: data in
    List.map data' ~f: (fun datum -> List.map datum ~f: (fun item ->
        match String.(index item '"', index item ';') with
        | None, None -> item
        | _, _ ->
            "\"" ^ Str.global_replace (Str.regexp "\"") "\"\"" item ^ "\""))
    |> List.map ~f: (String.concat ~sep: (String.of_char separator))

  (** Function to pretty-print the data. Can either print on each row, justified,
   * or on separate lines. This is tuned by a boolean (false by default).*)
  let pretty_print_data 
      ?(seplines=false) ?(fieldsep=';') ~linesep ~ids
      ~concatenate_first_lines data =
    match seplines with
    | false -> pretty_print_csv ~separator: fieldsep data
    | true -> pretty_print_linear ~linesep ~ids ~concatenate_first_lines data

  (** Function to compute row IDs for data.*)
  let ids_from_data =
    List.map ~f: (fun dat -> String.concat ~sep: (String.escaped "☯") dat
                             |> Digest.string |> Digest.to_hex)

  (** Main pretty-printer driver.*)
  let pprint_driver ?(row_selections=None) ?(col_selections=None) 
      ?(separator=';') ~seplines ?(linesep="\n")
      ?(concatenate_first_lines=1) ~input_file_name () =
    let rows, cols = 
      match row_selections, col_selections with
      | None, None -> None, None
      | Some rows', Some cols' -> Some (indexes_from_selection rows'), 
                                  Some (indexes_from_selection cols')
      | None, Some cols' -> None, Some (indexes_from_selection cols')
      | Some rows', None -> Some (indexes_from_selection rows'), None in
    let full_data = Csv.load ~separator input_file_name in
    let ids = ids_from_data full_data in
    pretty_print_data ~seplines ~linesep ~ids ~concatenate_first_lines
      ~fieldsep: separator (prune_data ~rows ~cols full_data)
end


let command =
  Command.basic
    ~summary: "Pretty print reporting data"
    ~readme: (fun () -> "=== Copyright © 2017 ELDA - All rights reserved ===\n")
    Command.Spec.(
      empty
      +> flag "--rows" (optional string) 
        ~doc: " Row selection (all rows by default)"
      +> flag "--columns" (optional string) 
        ~doc: " Column selection (all columns by default)"
      +> flag "-d" (optional_with_default ";" string)
        ~doc: " Input CSV file delimiter (; by default)"
      +> flag "--seplines" (optional_with_default true bool)
        ~doc: " Pretty print each column per line (true by default). \
                If false, then dump the data in the same CSV format as the \
                input data."
      +> flag "--linesep" (optional_with_default "\n" string)
        ~doc: " Entry line separator (\\n, blank line by default)."
      +> flag "--concat-first" (optional_with_default 1 int)
        ~doc: " Concatenate first entries into a single entry (1 by default, \
               hence no concatenation takes place)"
      +> flag "-i" (required string) ~doc: " Input report CSV file"
      +> flag "-o" (optional string) 
        ~doc: " Output file. If not provided, then input_file.txt is \
               produced. If provided, then output saved to that file."
    )
    (fun row_selections col_selections separator' seplines linesep
      concatenate_first_lines input_file_name output_file_name () ->
      let separator = Char.of_string separator' in
      let data =
        CrawlerPprinter.pprint_driver
          ~row_selections ~col_selections ~separator
          ~seplines ~linesep ~concatenate_first_lines
          ~input_file_name () |> String.concat ~sep: "\n" in
      let out_fname =
        match output_file_name with
        | None ->
          begin
            match seplines with
            | true ->
              let fname, _ = Filename.split_extension input_file_name in
              fname ^ ".txt"
            | false ->
              let fname, ext = Filename.split_extension input_file_name in
              let fname' =
                let row_sel =
                  match row_selections with
                  | None -> "all_rows"
                  | Some row_sel' -> row_sel' in
                let col_sel =
                  match col_selections with
                  | None -> "all_columns"
                  | Some col_sel' -> col_sel' in
                String.concat ~sep: "_" [fname; row_sel; col_sel] in
              let ext' =
                match ext with
                | None -> "csv"
                | Some ext'' -> ext'' in
              fname' ^ "." ^ ext'
          end
        | Some output_file_name' -> 
          if Filename.is_implicit output_file_name' then 
            Filename.concat 
              (Filename.realpath (Filename.dirname input_file_name)) 
              output_file_name' 
          else
            output_file_name' in
      Out_channel.write_all out_fname ~data)

let () = Command.run ~version: "1.0" ~build_info: "ELDA on Debian" command
