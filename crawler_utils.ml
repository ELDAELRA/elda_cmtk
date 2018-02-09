(**************************************************************************)
(* Copyright (C) 2018 Evaluations and Language Resources Distribution     *)
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

(* Generic common functionalities. *)

open Core
(** Generate indexes from numeric or character selections.*)
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

(** Retain a specified set of indexes from CSV-retrieved data.
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
        if List.mem rows' (i + 1) ~equal: Int.equal
          then Some (sorti datum cols') else None)
  | Some rows', None -> List.filteri data ~f: (
      fun i _ -> List.mem rows' (i + 1) ~equal: Int.equal)
  | None, Some cols' -> List.map data ~f: (fun dat -> sorti dat cols')
  | None, None -> data
