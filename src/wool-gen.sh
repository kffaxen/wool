#! /bin/bash

# Copyright notice:
echo '/*
   This file is part of Wool, a library for fine-grained independent
   task parallelism

   Copyright 2009- Karl-Filip Faxén, kff@sics.se
   All rights reserved.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are met:
       * Redistributions of source code must retain the above copyright
         notice, this list of conditions and the following disclaimer.
       * Redistributions in binary form must reproduce the above copyright
         notice, this list of conditions and the following disclaimer in the
         documentation and/or other materials provided with the distribution.
       * Neither "Wool" nor the names of its contributors may be used to endorse
         or promote products derived from this software without specific prior
         written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
   ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
   DISCLAIMED. IN NO EVENT SHALL THE AUTHOR OR OTHER CONTRIBUTORS BE LIABLE FOR ANY
   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
   (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
   LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   This is Wool version @WOOL_VERSION@
*/

#ifndef WOOL_H
#define WOOL_H

#include "wool-common.h"
'

#
# Second part, once for each arity
#

ARGS_MAX_ALIGN="1"
OFFSET_EXP="0"

for(( r = 0; r <= $1; r++ )) do

# Naming various argument lists

if ((r)); then
  MACRO_ARGS="$MACRO_ARGS, ATYPE_$r, ARG_$r"
  MACRO_a_ARGS="$MACRO_a_ARGS, ATYPE_$r, a$r"
  MACRO_DECL_ARGS="$MACRO_DECL_ARGS, ATYPE_$r"
  WRK_FORMALS="$WRK_FORMALS, ATYPE_$r ARG_$r"
  FUN_a_FORMALS="$FUN_a_FORMALS, ATYPE_$r a$r"
  CALL_a_ARGS="$CALL_a_ARGS, a$r"
  ARG_TYPES="$ARG_TYPES, ATYPE_$r"
  ARGS_MAX_ALIGN="_WOOL_(max)( __alignof__(ATYPE_$r), $ARGS_MAX_ALIGN )"

  PTR_TO_CURRENT_FIELD_p="*(ATYPE_$r *)( _WOOL_(p) + _WOOL_ALIGNTO( $OFFSET_EXP, ATYPE_$r ) )"
  TASK_a_INIT_p="$TASK_a_INIT_p  $PTR_TO_CURRENT_FIELD_p = a$r;
"
  TASK_GET_FROM_p="$TASK_GET_FROM_p, $PTR_TO_CURRENT_FIELD_p"
  OFFSET_EXP="_WOOL_OFFSET_AFTER( $OFFSET_EXP, ATYPE_$r )"
fi


if ((r)); then

echo
echo "// Task definition for arity $r"
echo

(\
echo \
"#define TASK_FORW_$r(RTYPE, NAME$MACRO_DECL_ARGS )
  static inline __attribute__((__always_inline__))
    void NAME##_SPAWN_DSP( Worker *__self, int _WOOL_(fs_in_task)$ARG_TYPES);
  static inline __attribute__((__always_inline__))
    RTYPE NAME##_SYNC_DSP( Worker *__self, int _WOOL_(fs_in_task) );
  static inline __attribute__((__always_inline__))
    RTYPE NAME##_CALL_DSP( Worker *__self, int _WOOL_(fs_in_task)$ARG_TYPES);"
) | awk '{printf "%-70s\\\n", $0 }'

echo

(\
echo \
"#define VOID_TASK_FORW_$r(NAME$MACRO_DECL_ARGS )
  static inline __attribute__((__always_inline__))
    void NAME##_SPAWN_DSP( Worker *__self, int _WOOL_(fs_in_task)$ARG_TYPES);
  static inline __attribute__((__always_inline__))
    void NAME##_SYNC_DSP( Worker *__self, int _WOOL_(fs_in_task) );
  static inline __attribute__((__always_inline__))
    void NAME##_CALL_DSP( Worker *__self, int _WOOL_(fs_in_task)$ARG_TYPES);"
) | awk '{printf "%-70s\\\n", $0 }'

echo

for isvoid in 0 1; do

if (( isvoid==0 )); then
  DEF_MACRO_LHS="#define TASK_$r(RTYPE, NAME$MACRO_ARGS )"
  DCL_MACRO_LHS="#define TASK_DECL_$r(RTYPE, NAME$MACRO_DECL_ARGS)"
  IMP_MACRO_LHS="#define TASK_IMPL_$r(RTYPE, NAME$MACRO_ARGS )"
  DEF_MACRO_RHS="TASK_DECL_$r(RTYPE, NAME$MACRO_DECL_ARGS)
TASK_IMPL_$r(RTYPE, NAME$MACRO_ARGS)"
  RTYPE="RTYPE"
  RES_FIELD="$RTYPE res;
"
  SAVE_RVAL="t->d.res ="
  SAVE_TO_res="res ="
  SAVE_FROM_res="post_eval_task->d.res = res;
"
  RETURN_RES_cached_top="( (NAME##_TD *) cached_top )->d.res"
  ASSIGN_RES="res = "
  RES_VAR="res"
  TASK_SIZE="sizeof(NAME##_TD)"
else
  DEF_MACRO_LHS="#define VOID_TASK_$r(NAME$MACRO_ARGS )"
  DCL_MACRO_LHS="#define VOID_TASK_DECL_$r(NAME$MACRO_DECL_ARGS)"
  IMP_MACRO_LHS="#define VOID_TASK_IMPL_$r(NAME$MACRO_ARGS )"
  DEF_MACRO_RHS="VOID_TASK_DECL_$r(NAME$MACRO_DECL_ARGS)
VOID_TASK_IMPL_$r(NAME$MACRO_ARGS)"
  RTYPE="void"
  RES_FIELD=""
  SAVE_RVAL=""
  SAVE_TO_res=""
  SAVE_FROM_res=""
  RETURN_RES_cached_top=""
  ASSIGN_RES=""
  RES_VAR=""
  TASK_SIZE="0"
fi

(\
echo "$DCL_MACRO_LHS

typedef struct _##NAME##_TD {
  TASK_COMMON_FIELDS( struct _##NAME##_TD * )
  union {
    $RES_FIELD
  } d;
} NAME##_TD;

typedef struct {
  Task* (*f)(Worker *__self, NAME##_TD *t);
  int size;
} NAME##_DICT_T;

static inline __attribute__((__always_inline__))
char* NAME##_FREE_SPACE(Task* cached_top)
{
  char *_WOOL_(p) = _WOOL_(arg_ptr)( cached_top, $ARGS_MAX_ALIGN );

  return _WOOL_(p) + _WOOL_ALIGNTO( $OFFSET_EXP, double );
}

/** SPAWN related functions **/

Task* NAME##_WRAP(Worker *__self, NAME##_TD *t);

extern NAME##_DICT_T NAME##_DICT;

static inline __attribute__((__always_inline__))
void NAME##_SPAWN(Worker *__self $FUN_a_FORMALS)
{
  Task* cached_top = __self->pr.pr_top;
  char *_WOOL_(p) = _WOOL_(arg_ptr)( cached_top, $ARGS_MAX_ALIGN );

$TASK_a_INIT_p

  COMPILER_FENCE;

  _WOOL_(fast_spawn)( __self, cached_top, (_wool_task_header_t) &NAME##_DICT );

}

static inline __attribute__((__always_inline__))
void NAME##_SPAWN_DSP(Worker *__self, int _WOOL_(fs_in_task)$FUN_a_FORMALS)
{
  if( _WOOL_(fs_in_task) ) {
    NAME##_SPAWN( __self $CALL_a_ARGS );
  } else {
    __self = _WOOL_(slow_get_self)( );
    NAME##_SPAWN( __self $CALL_a_ARGS );
  }
}

/** CALL related functions **/

$RTYPE NAME##_CALL(Worker *_WOOL_(self) $FUN_a_FORMALS);

static inline __attribute__((__always_inline__))
$RTYPE NAME##_CALL_DSP( Worker *_WOOL_(self), int _WOOL_(fs_in_task)$FUN_a_FORMALS )
{
  if( _WOOL_(fs_in_task) ) {
    return NAME##_CALL( _WOOL_(self) $CALL_a_ARGS );
  } else {
    _WOOL_(self) = _WOOL_(slow_get_self)( );
    return NAME##_CALL( _WOOL_(self) $CALL_a_ARGS );
  }
}

/** SYNC related functions **/

/* This implementation has the PUB function only in the implementation file, so uses from other
   compilation units call it.
*/
Task *NAME##_PUB(Worker *self, Task *top, Task *jfp );

static inline __attribute__((__always_inline__))
$RTYPE NAME##_SYNC(Worker *__self)
{
  WOOL_WHEN_MSPAN( hrtime_t e_span; )
  Task *jfp = __self->pr.join_first_private;
  Task *cached_top = __self->pr.pr_top;

  if( MAKE_TRACE ||
      ( LOG_EVENTS &&
      __self->pr.curr_block_fidx + ( cached_top - __self->pr.curr_block_base ) <= __self->pr.n_public ) )
  {
    logEvent( __self, 6 );
  }

  if( __builtin_expect( jfp < cached_top, 1 ) ) {
    Task *t = --cached_top;
    char *_WOOL_(p) = _WOOL_(arg_ptr)( t, $ARGS_MAX_ALIGN );
    $RES_FIELD

    __self->pr.pr_top = cached_top;
    PR_INC( __self, CTR_inlined );

    WOOL_MSPAN_BEFORE_INLINE( e_span, t );

    $ASSIGN_RES NAME##_CALL( __self $TASK_GET_FROM_p );
    WOOL_MSPAN_AFTER_INLINE( e_span, t );
    if( MAKE_TRACE ) {
      logEvent( __self, 8 );
    }
    return $RES_VAR;
  } else {
    cached_top = NAME##_PUB( __self, cached_top, jfp );
    return $RETURN_RES_cached_top;
  }
}

static inline __attribute__((__always_inline__))
$RTYPE NAME##_SYNC_DSP( Worker *__self, int _WOOL_(fs_in_task) )
{
  if( _WOOL_(fs_in_task) ) {
    return NAME##_SYNC( __self );
  } else {
    __self = _WOOL_(slow_get_self)( );
    return NAME##_SYNC( __self );
  }
}" \

) | awk '{printf "%-70s\\\n", $0 }'

echo ""

(\
echo "$IMP_MACRO_LHS

$RTYPE NAME##_CALL(Worker *_WOOL_(self) $FUN_a_FORMALS);

/** SPAWN related functions **/

Task* NAME##_WRAP_AUX(Worker *__self, NAME##_TD *t $FUN_a_FORMALS)
{
  NAME##_TD *post_eval_task;
  NAME##_TD *volatile v_t = t;
  $RES_FIELD

  COMPILER_FENCE;
  _WOOL_(save_link)( (Task**) &v_t );

  $SAVE_TO_res NAME##_CALL( __self $CALL_a_ARGS );

  post_eval_task = (NAME##_TD*) _WOOL_(swap_link)( (Task**) &v_t, NULL );
  $SAVE_FROM_res

  return (Task *) post_eval_task;
}

Task* NAME##_WRAP(Worker *__self, NAME##_TD *t)
{
  char *_WOOL_(p) = _WOOL_(arg_ptr)( (Task *) t, $ARGS_MAX_ALIGN );
  return NAME##_WRAP_AUX( __self, t $TASK_GET_FROM_p );
}

NAME##_DICT_T NAME##_DICT = { &NAME##_WRAP, $TASK_SIZE };

/** SYNC related functions **/

Task *NAME##_PUB(Worker *self, Task *top, Task *jfp )
{
  unsigned long ps = self->pr.public_size;

  WOOL_WHEN_AS( int us; )

  grab_res_t res = WOOL_FAST_EXC ? TF_EXC : TF_OCC;

  if(
        ( WOOL_WHEN_AS_C( us = self->pr.unstolen_stealable )
         __builtin_expect( (unsigned long) jfp - (unsigned long) top < ps, 1 ) )
         && __builtin_expect( WOOL_LS_TEST(us), 1 )
         && (res = _WOOL_(grab_in_sync)( self, (top)-1 ),
             (
               WOOL_WHEN_AS_C( self->pr.unstolen_stealable = us-1 )
               __builtin_expect( res != TF_OCC, 1 ) ) )
   ) {
    /* Semi fast case */
    NAME##_TD *t = (NAME##_TD *) --top;
    char *_WOOL_(p) = _WOOL_(arg_ptr)( (Task *) t, $ARGS_MAX_ALIGN );

    self->pr.pr_top = top;
    PR_INC( self, CTR_inlined );
    $SAVE_RVAL NAME##_CALL( self $TASK_GET_FROM_p );
    return top;
  } else {
      /* An exceptional case */
      top = _WOOL_(slow_sync)( self, top, res );
      return top;
  }

}

/** CALL related functions **/

static inline __attribute__((__always_inline__))
$RTYPE NAME##_WRK(Worker *, int _WOOL_(in_task)$WRK_FORMALS);

$RTYPE NAME##_CALL(Worker *_WOOL_(self) $FUN_a_FORMALS)
{
  return NAME##_WRK( _WOOL_(self), 1 $CALL_a_ARGS );
}

static inline __attribute__((__always_inline__))
$RTYPE NAME##_WRK(Worker *__self, int _WOOL_(in_task)$WRK_FORMALS)"\

) | awk '{printf "%-70s\\\n", $0 }'

echo ""

echo "$DEF_MACRO_LHS
$DEF_MACRO_RHS
" | awk '{printf "%-70s\\\n", $0 }'

echo ""
done
fi

if ((r < $1-1)); then
(\
echo "\
#define LOOP_BODY_$r(NAME, COST, IXTY, IXNAME$MACRO_ARGS)

static unsigned long const NAME##__min_iters__
   = COST > FINEST_GRAIN ? 1 : FINEST_GRAIN / ( COST ? COST : 20 );

static inline void NAME##_LOOP(Worker *__self, IXTY IXNAME$WRK_FORMALS);

VOID_TASK_$((r+2))(NAME##_TREE, IXTY, __from, IXTY, __to$MACRO_a_ARGS)
{
  if( __to - __from <= NAME##__min_iters__ ) {
    IXTY __i;
    for( __i = __from; __i < __to; __i++ ) {
      NAME##_LOOP( __self, __i$CALL_a_ARGS );
    }
  } else {
    IXTY __mid = (__from + __to) / 2;
    SPAWN( NAME##_TREE, __mid, __to$CALL_a_ARGS );
    CALL( NAME##_TREE, __from, __mid$CALL_a_ARGS );
    SYNC( NAME##_TREE );
  }
}

static inline void NAME##_LOOP(Worker *__self, IXTY IXNAME$WRK_FORMALS)"\
) | awk '{printf "%-70s\\\n", $0 }'

fi

done

echo '
#endif'

