-module(macros).

-include_lib("erlavro/include/avro_internal.hrl").

-export([default_decoder_hook/0, is_record_type/1, name/1, avro_long/0, avro_bytes/0, avro_string/0, line/0]).

default_decoder_hook() ->
    ?DEFAULT_DECODER_HOOK.

is_record_type(Type) ->
    ?IS_RECORD_TYPE(Type).

name(TypeName) ->
    ?NAME(TypeName).

avro_long() ->
    ?AVRO_LONG.

avro_bytes() ->
    ?AVRO_BYTES.

avro_string() ->
    ?AVRO_STRING.

line() ->
    ?LINE.
