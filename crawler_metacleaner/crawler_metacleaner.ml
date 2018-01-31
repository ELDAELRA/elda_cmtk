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

(** Tool to clean up duplicate crawled URLs.
    Issue: sometimes URLs like www.toto.fr/titi and www.toto.fr/tata are crawled
    as if they were independent sites. Actually, the crawler crawls www.toto.fr
    twice, in order to get a maximum number of TUs.

    Thus, the goal of this tool is to look through the batches and as soon as it
    encounters duplicate *base* URLs (i.e. www.toto.fr), only the first one is
    kept and the subsequent ones are added to a _duplicates directory.
*)
open Core

module CrawlerMetaCleaner : sig 
  val clean_duplicates: ?regex: bool -> ?dest: string option ->
    keep_newest: bool -> string -> unit -> unit
end = struct

  (** Helper to get regexp out of a glob pattern.
      Courtesy of http://pleac.sourceforge.net/pleac_ocaml/directories.html*)
  let regexp_of_glob pattern =
    Str.regexp
      (Printf.sprintf "^%s$"
         (String.concat ~sep:""
            (List.map ~f:
               (function
                 | Str.Text s -> Str.quote s
                 | Str.Delim "*" -> ".*"
                 | Str.Delim "?" -> "."
                 | Str.Delim _ -> assert false)
               (Str.full_split (Str.regexp "[*?]") pattern))))

  (** Helper to recursively get list of dirs until the first file is met or empty
      dir is met.*)
  let rec get_dirs' root acc =
    if Sys.is_file_exn root then acc
    else List.map (Sys.ls_dir root) ~f: (fun x ->
        let acc' = if not (List.mem acc root ~equal: String.equal) 
                    then root :: acc else acc in
        get_dirs' (Filename.concat root x) acc')
         |> List.concat
         |> List.dedup
         |> List.filter ~f: (fun dir ->
             List.exists (Sys.ls_dir dir) ~f: (
               fun sdir ->
                 let is_good_file fname =
                   match
                     Pcre.extract
                       ~rex: (Pcre.regexp {|output_data_[a-z]{3,3}-[a-z]{3,3}\.tmx|})
                       fname
                   with
                   | exception Not_found -> false
                   | _ -> true in
                 Sys.is_file_exn (Filename.concat dir sdir) && is_good_file sdir))

  (** Helper to drive the recursive directory getter.*)
  let get_dirs root =
    get_dirs' root []

  (** Helper to look through the crawler batches according to a pattern.
      Equivalent of find base_pattern -type d -maxdepth 1*)
  let get_crawled_dirs ~regex root_pattern =
    let pattern_rex =
      if regex = false then regexp_of_glob root_pattern
      else Str.regexp root_pattern in
    let roots = 
      if Sys.file_exists_exn root_pattern then
        if Filename.is_implicit root_pattern then
          let root = Filename.(concat current_dir_name root_pattern |> realpath)
          in
          if Sys.is_directory_exn root then [root] else []
        else
          let root = Filename.realpath root_pattern in
          if Sys.is_directory_exn root then [root] else []
      else
        let curr_dir, _ = Filename.split root_pattern in
        Sys.ls_dir curr_dir
        |> List.map ~f: (fun sdir -> Filename.concat curr_dir sdir)
        |> List.sort ~cmp: String.compare in
    List.map roots ~f: get_dirs
    |> List.concat
    |> List.filter ~f: (fun dir ->
        match Str.search_forward pattern_rex dir 0 with
        | exception Not_found -> false
        | _ -> true)


  (** Interface function to move duplicates according to a pattern.*)
  let clean_duplicates ?(regex=false) ?(dest=None) ~keep_newest
      root () =
    let sign =
      match keep_newest with
      | false -> ((-) 0)
      | true -> ((+) 0) in
    let crawled_dirs =
      get_crawled_dirs ~regex root
      |> List.sort ~cmp: (fun dir_x dir_y ->
          let modif_time dir =
            Unix.((stat dir).st_mtime)
            |> Time.Span.of_sec
            |> Time.of_span_since_epoch in
          sign (Time.compare (modif_time dir_x) (modif_time dir_y))) in
    let unique_crawled_dirs =
      List.dedup crawled_dirs ~compare: (fun x y ->
          String.compare 
            (Filename.basename x |> String.split ~on: '_' |> List.hd_exn)
            (Filename.basename y |> String.split ~on: '_' |> List.hd_exn)) in
    let to_move =
      String.Set.diff 
        (String.Set.of_list crawled_dirs)
        (String.Set.of_list unique_crawled_dirs)
      |> String.Set.to_list in
    let dest_dir =
      match dest with
      | Some dest' -> dest'
      | None ->
        let curr_dir, base_pat = Filename.split root in
        let clean_pat = Str.global_replace (Str.regexp "[][.*?-]") "" base_pat in
        Filename.(concat (realpath curr_dir) "duplicate_" ^ clean_pat) in
    begin
      Printf.printf "%d duplicate sites found\n" (List.length to_move);
      List.iter to_move ~f: (
        fun in_dir ->
          Unix.mkdir_p dest_dir;
          let out_dir = 
            let out_dir' = Filename.(concat dest_dir @@ basename in_dir) in
            let out_candidates =
              try
                Sys.ls_dir dest_dir
                |> List.sort ~cmp: String.compare
                |> List.filter_map ~f: (fun sdir ->
                    match Str.search_forward
                            (Str.regexp (
                                (Str.quote (Filename.basename in_dir)) ^
                                {|\($\|\.[0-9]+$\)|}))
                            sdir 0 with
                    | exception Not_found -> None
                    | _ -> Some sdir) 
              with
              | Sys_error _ -> [] in
            if List.length out_candidates = 0 then out_dir'
            else
              let index =
                match
                  List.last_exn out_candidates
                  |> String.split ~on: '.'
                  |> List.filter_map ~f: (fun bit ->
                      match Str.search_backward (Str.regexp "^[0-9]+$") bit 
                              (String.length bit) with
                      | exception Not_found -> None
                      | _ -> Some bit) 
                  |> List.last with
                | None -> 1
                | Some idx -> 1 + Int.of_string idx in
              out_dir' ^ "." ^ Int.to_string index in
          Printf.printf "Moving \n  %s to\n  %s\n" in_dir out_dir;
          Unix.rename ~src: in_dir ~dst: out_dir
      )
    end
end

let command =
  Command.basic_spec
    ~summary: "Clean-up duplicate crawled sites across batches"
    ~readme: (fun () -> "=== Copyright © 2016 ELDA - All rights reserved ===\n")
    Command.Spec.(
      empty
      +> anon ("dir-pattern" %: string)
      +> flag "--regex" (optional_with_default false bool)
        ~doc: " Use regular expression in the pattern, instead of glob \
               (false by default)"
      +> flag "-o" (optional string) 
        ~doc: " Output directory (duplicate_fixed_part_of_the_dir-pattern by \
               default)"
      +> flag "--keep-newest" (optional_with_default false bool)
        ~doc: " Keeps the newest duplicate crawled site and discards the older
               ones to the output directory holding duplicate sites (false by \
               default)"
    )
    (fun dir_pattern regex out_dir keep_newest ->
       CrawlerMetaCleaner.clean_duplicates dir_pattern ~regex ~dest: out_dir
         ~keep_newest)

let () = Command.run ~version: "1.0" ~build_info: "ELDA on Debian" command
