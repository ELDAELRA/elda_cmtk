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

(** This tool performs backups of the crawled data, by:

    1. Browsing a given root directory and looking for crawling batch patterns.
    2. For each pattern, selecting the .xml, .html, .txt and .tmx files to be
       zipped.
    3. Zipping all these files and putting the zip file to a specified
       destination directory.

*)
open Core.Std

module CrawlerUtils : sig 
  val walk: ?depth: int -> string -> string list
  val prune_dirtree: extensions: string list -> string list 
    -> string list
  val get_batches: string -> string -> string list
end = struct

  (** Utility function for recursively traversing a directory
   *  Inspired from RWO / chapter 3.
   * *)
  let rec walk ?(depth=Int.max_value) root =
    (**XXX: Duplicate with eponymous functionality from crawler_reporter!*)
    if depth > 0 then
      if Sys.is_file_exn ~follow_symlinks: true root then [root]
      else
        Sys.ls_dir root
        |> List.map ~f:(fun sub -> walk (Filename.concat root sub)
                           ~depth: (depth - 1))
        |> List.concat
    else [root]

  (** Helper to prune a directory tree, on a list of extensions.*)
  let prune_dirtree ~extensions filelist =
    List.filter ~f: (
      fun entry -> 
        List.exists extensions ~f: (
          fun ext ->
            let _, ext'= Filename.split_extension entry in
            match ext' with
            | None -> false
            | Some ext'' -> if ext'' = ext then true else false))
      filelist

  (** Simple pattern matcher.*)
  let matches_pattern ~pattern str =
    let glob = Re_glob.globx pattern |> Re.compile in
    Re.execp glob str

  (**Recursive helper to get crawler batches from pattern and root dir.
     XXX: Not tail-recursive.*)
  let rec get_batches' root pattern acc =
    try
      let subdirs =
        Sys.ls_dir root
        |> List.filter ~f: (
          Fn.compose Sys.is_directory_exn (Filename.concat root)) in
      let subdirs' = List.filter subdirs ~f: (matches_pattern ~pattern) in
      if List.(length subdirs' = length subdirs) then
        List.map subdirs' ~f: (Filename.concat root) @ acc
      else
        let subdirs'' =
          String.Set.diff
            (String.Set.of_list subdirs)
            (String.Set.of_list subdirs')
          |> String.Set.to_list in
        List.map subdirs'' ~f: (
          fun sd ->
            get_batches'
              (Filename.concat root sd)
              pattern
              (List.map subdirs' ~f: (Filename.concat root) @ acc))
        |> List.concat
        |> List.dedup
    with
    | Sys_error _ -> []

  (** Helper to get crawler batches from pattern and root dir*)
  let get_batches root pattern =
    get_batches' root pattern []
end

module CrawlerZip = struct

  (** Interface function to actually create a zip file.*)
  let make_zip ~zipname ?(destination_directory=None) files =
    if files <> [] then
      let dest_dir =
        match destination_directory with
        | None -> Sys.getcwd ()
        | Some dir -> Filename.realpath dir in
      let zip_full_name =
        Filename.concat dest_dir zipname in
      let open Async.Std in
      let handle =
        Process.run_exn ~prog: "zip" ~args: (zip_full_name :: files) () in
      Thread_safe.block_on_async_exn (fun () -> handle) |> ignore
end

module CrawlerBackuper =
struct
  (**Algorithm: find all batches, then, for each batch, walk,
     filter on extension, and zip.*)
  let backup_batches
      ?(extensions=["txt"; "tmx"; "xml"; "html"])
      ~root_directory ~batch_name_pattern ~backup_directory () =
    let open CrawlerUtils in
    let open CrawlerZip in
    let batches = get_batches root_directory batch_name_pattern in
    (* Split a list into sublists of length (at most) n. We don't care about the
     * order of the files in the sublists, hence, for efficiency reasons, we
     * don't do a List.rev on the result of the call to the auxiliary
     * function. *)
    let iter_split_n alist n =
      let rec iter_split_n' alist n splits =
        match alist with
        | [] -> splits
        | _ ->
            let hd, tail = List.split_n alist n in
            iter_split_n' tail n (hd :: splits) in
      iter_split_n' alist n [] in
    Parmap.pariter (fun batch ->
        let zipname = Filename.basename batch ^ ".zip" in
        let files_to_zip = walk batch |> prune_dirtree ~extensions  in
        (* Honor the ML_ARG_MAX = 4096 + 1. *)
        if List.length files_to_zip <= 4094 then
          make_zip
            ~zipname ~destination_directory: (Some backup_directory)
            files_to_zip
        else
          (* Decompose files_to_zip in <= 4094-length lists and iterate make_zip
           * on each such sublist.*)
          let splits = iter_split_n files_to_zip 4094 in
          List.iter ~f: (fun files_to_zip_chunk ->
            make_zip ~zipname ~destination_directory: (Some backup_directory)
                     files_to_zip_chunk)
          splits
      ) (Parmap.L batches)
end

let command =
  Command.basic
    ~summary: "Back-up crawled data"
    ~readme: (fun () -> "=== Copyright © 2016 ELDA - All rights reserved ===\n")
    Command.Spec.(
      empty
      +> flag "-r" (required string)
        ~doc: " Root directory to look for crawling data"
      +> flag "--pattern" (required string)
        ~doc: " Pattern of the directories containing crawling data (a.k.a. \
               batches)"
      +> flag "-b" (required string)
        ~doc: " Back-up directory where all per-batch archives are placed"
      +> flag "--file-types" (optional string)
        ~doc: " File types to be backed-up (.txt, .tmx, .xml and .html by \
               default)"
    )
    (fun root_directory batch_name_pattern backup_directory extensions ->
       let open CrawlerBackuper in
       match extensions with
       | None ->
         (fun () ->
            backup_batches
              ~root_directory ~batch_name_pattern ~backup_directory ())
       | Some extensions' ->
         let extensions = String.split extensions' ~on: ';' in
         backup_batches
           ~extensions ~root_directory ~batch_name_pattern
           ~backup_directory)

let () = Command.run ~version: "1.0" ~build_info: "ELDA on Debian" command
