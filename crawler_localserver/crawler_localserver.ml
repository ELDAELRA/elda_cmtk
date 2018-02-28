(**************************************************************************)
(* Copyright (C) 2017 Evaluations and Language Resources Distribution     *)
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

(** Tool for serving third-party documents locally.
 *
 *  To this end:
 *  1. List all files in all subdirectories.
 *  2. For each listing, convert it into an URL relative to the current
 *     directory and with a specified prefix (root URL / domain, port).
 *  3. For each listing, type into an li element which contains an a href.
 *  4. Add necessary HTML boilerplate and grab the contents generated at 3, and
 *     generate index HTML page.
 *  5. Launch cohttp-server-async with the index (as built at 4) and port
 *     chosen.
 *  6. If cohttp-server-async is missing, fall back on python3 -m http.server.
 *     *)
open Core
open Re2.Std

open Cohttp_async
open Tyxml

exception Elda_cmtk_localserver_error of string

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

(** Build URLs from directory listing and domain and port information. *)
let urls_from_directory ?(domain="http://localhost") ~port root_directory =
  if Fn.non Sys.is_directory_exn root_directory then
    raise (Elda_cmtk_localserver_error
            (Printf.sprintf "%s is not a directory" root_directory))
  else
    let documents = walk root_directory in
    List.map documents ~f: (fun doc ->
      String.concat ~sep: ":" [domain; Int.to_string port]
      ^/ String.concat @@ Re2.split (Re2.create_exn root_directory) doc)

(** Build HTML structure from URL listing and specified title. *)
let html_from_urls ~page_title urls =
  let open Html in
  html
    (head (title (pcdata page_title)) [])
    (body [div
            [ul (List.map urls ~f: (fun url ->
                  let docname = Filename.basename url in
                  li [a ~a: [a_href url] [pcdata docname]]))]])

(** Serialize HTML to web page. *)
let web_page_from_html =
  Format.asprintf "%a" (Html.pp ())

(** Cleanup on Ctrl-C. *)
let cleanup_sigint filename =
  Caml.Sys.set_signal
    Caml.Sys.sigint
    (Caml.Sys.Signal_handle
      (fun _signum ->
        Unix.unlink filename;
        Printf.printf "\n";
        exit 0))

(** Build temporary index file into specified directory and launch the server
 * inside it. *)
let launch_server ~page_title ~docroot ~port () =
  let index_html =
    urls_from_directory ~port docroot
    |> html_from_urls ~page_title
    |> web_page_from_html
  in
  let temp_index =
    Filename.temp_file ~in_dir: (Filename.realpath docroot) "auto" ".html"
  in
  Out_channel.write_all temp_index index_html;
  begin
    begin
      let open Async in
      let handle =
        try_with
          (fun () ->
            cleanup_sigint temp_index;
            Process.run_expect_no_output_exn
              ~prog: "cohttp-server-async"
              ~args: [Filename.realpath docroot; "-i";
                      Filename.basename temp_index; "-p";
                      Int.to_string port] ())
          >>= function
          | Error _ ->
              let fallback_index =
                Filename.concat (Filename.dirname temp_index) "index.html" in
              Core.Unix.rename ~src: temp_index ~dst: fallback_index;
              cleanup_sigint fallback_index;
              Process.run_expect_no_output_exn
                ~prog: "python3"
                ~args: ["-m"; "http.server"; Int.to_string port]
                ~working_dir: docroot ()
          | Ok _ -> return ()
      in
      Thread_safe.block_on_async_exn (fun () -> handle)
    end
  end

let command =
  Command.basic_spec
    ~summary: "Automatically serve locally third-party documents"
    ~readme: (fun () -> "=== Copyright © 2017 ELDA - All rights reserved ===\n")
    Command.Spec.(
      empty
      +> flag "-d" (required string) ~doc: " Document root directory"
      +> flag "-p" (optional_with_default 8000 int)
          ~doc: " Server port (8000 by default)"
      +> flag "-t" (optional_with_default "Default title" string)
          ~doc: " Index title (\"Default title\" by default)"
    )
    (fun docroot port page_title -> launch_server ~page_title ~docroot ~port)

let () =
  Command.run ~version: "1.2.1" ~build_info: "ELDA on Debian" command
