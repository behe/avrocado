-module(macros).

-compile(export_all).

-include_lib("erlavro/include/avro_internal.hrl").

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
