% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(fabric2_db_size_tests).


-include_lib("couch/include/couch_db.hrl").
-include_lib("couch/include/couch_eunit.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("fabric2_test.hrl").


db_size_test_() ->
    {
        "Test document CRUD operations",
        {
            setup,
            fun setup/0,
            fun cleanup/1,
            with([
                ?TDEF(empty_size),
                ?TDEF(new_doc),
                ?TDEF(edit_doc),
                ?TDEF(del_doc),
                ?TDEF(conflicted_doc),
                ?TDEF(del_conflict)
            ])
        }
    }.


setup() ->
    Ctx = test_util:start_couch([fabric]),
    {ok, Db} = fabric2_db:create(?tempdb(), [{user_ctx, ?ADMIN_USER}]),
    {Db, Ctx}.


cleanup({Db, Ctx}) ->
    ok = fabric2_db:delete(fabric2_db:name(Db), []),
    test_util:stop_couch(Ctx).


empty_size({Db, _}) ->
    ?assertEqual(2, db_size(Db)).


new_doc({Db, _}) ->
    increases(Db, fun() ->
        create_doc(Db)
    end).


edit_doc({Db, _}) ->
    DocId = fabric2_util:uuid(),
    {ok, RevId1} = increases(Db, fun() ->
        create_doc(Db, DocId)
    end),
    {ok, RevId2} = increases(Db, fun() ->
        update_doc(Db, DocId, RevId1, {[{<<"foo">>, <<"bar">>}]})
    end),
    decreases(Db, fun() ->
        update_doc(Db, DocId, RevId2)
    end).


del_doc({Db, _}) ->
    DocId = fabric2_util:uuid(),
    {ok, RevId} = increases(Db, fun() ->
        create_doc(Db, DocId, {[{<<"foo">>, <<"bar">>}]})
    end),
    % The change here is -11 becuase we're going from
    % {"foo":"bar"} == 13 bytes to {} == 2 bytes.
    % I.e., 2 - 13 == -11
    diff(Db, fun() ->
        delete_doc(Db, DocId, RevId)
    end, -11).


conflicted_doc({Db, _}) ->
    DocId = fabric2_util:uuid(),
    Before = db_size(Db),
    {ok, RevId1} = increases(Db, fun() ->
        create_doc(Db, DocId, {[{<<"foo">>, <<"bar">>}]})
    end),
    Between = db_size(Db),
    increases(Db, fun() ->
        create_conflict(Db, DocId, RevId1, {[{<<"foo">>, <<"bar">>}]})
    end),
    After = db_size(Db),
    ?assertEqual(After - Between, Between - Before).


del_conflict({Db, _}) ->
    DocId = fabric2_util:uuid(),
    {ok, RevId1} = increases(Db, fun() ->
        create_doc(Db, DocId, {[{<<"foo">>, <<"bar">>}]})
    end),
    {ok, RevId2} = increases(Db, fun() ->
        create_conflict(Db, DocId, RevId1, {[{<<"foo">>, <<"bar">>}]})
    end),
    decreases(Db, fun() ->
        {ok, RevId3} = delete_doc(Db, DocId, RevId2),
        ?debugFmt("~p ~p ~p", [RevId1, RevId2, RevId3])
    end).


create_doc(Db) ->
    create_doc(Db, fabric2_util:uuid()).


create_doc(Db, DocId) when is_binary(DocId) ->
    create_doc(Db, DocId, {[]});
create_doc(Db, {Props} = Body) when is_list(Props) ->
    create_doc(Db, fabric2_util:uuid(), Body).


create_doc(Db, DocId, Body) ->
    Doc = #doc{
        id = DocId,
        body = Body
    },
    fabric2_db:update_doc(Db, Doc).


create_conflict(Db, DocId, RevId) ->
    create_conflict(Db, DocId, RevId, {[]}).


create_conflict(Db, DocId, RevId, Body) ->
    {Pos, _} = RevId,
    % Only keep the first 16 bytes of the UUID
    % so that we match the normal sized revs
    <<NewRev:16/binary, _/binary>> = fabric2_util:uuid(),
    Doc = #doc{
        id = DocId,
        revs = {Pos, [NewRev]},
        body = Body
    },
    fabric2_db:update_doc(Db, Doc, [replicated_changes]).


update_doc(Db, DocId, RevId) ->
    update_doc(Db, DocId, RevId, {[]}).


update_doc(Db, DocId, {Pos, Rev}, Body) ->
    Doc = #doc{
        id = DocId,
        revs = {Pos, [Rev]},
        body = Body
    },
    fabric2_db:update_doc(Db, Doc).


delete_doc(Db, DocId, RevId) ->
    delete_doc(Db, DocId, RevId, {[]}).


delete_doc(Db, DocId, {Pos, Rev}, Body) ->
    Doc = #doc{
        id = DocId,
        revs = {Pos, [Rev]},
        deleted = true,
        body = Body
    },
    fabric2_db:update_doc(Db, Doc).


constant(Db, Fun) -> check(Db, Fun, fun erlang:'=='/2).
increases(Db, Fun) -> check(Db, Fun, fun erlang:'>'/2).
decreases(Db, Fun) -> check(Db, Fun, fun erlang:'<'/2).
diff(Db, Fun, Change) -> check(Db, Fun, fun(A, B) -> (A - B) == Change end).

check(Db, Fun, Cmp) ->
    Before = db_size(Db),
    Result = Fun(),
    After = db_size(Db),
    ?debugFmt("~p :: ~p ~p", [erlang:fun_info(Cmp), After, Before]),
    ?assert(Cmp(After, Before)),
    Result.


db_size(Info) when is_list(Info) ->
    {sizes, {Sizes}} = lists:keyfind(sizes, 1, Info),
    {<<"external">>, External} = lists:keyfind(<<"external">>, 1, Sizes),
    External;
db_size(Db) when is_map(Db) ->
    {ok, Info} = fabric2_db:get_db_info(Db),
    db_size(Info).
