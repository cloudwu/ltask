/*
	modify from https://github.com/cloudwu/lua-serialize
 */

#ifdef TEST_SERI

#define LUA_LIB

#endif

#include <lua.h>
#include <lauxlib.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>

#define TYPE_BOOLEAN 0

#define TYPE_BOOLEAN_NIL 0
#define TYPE_BOOLEAN_FALSE 1
#define TYPE_BOOLEAN_TRUE 2

// hibits 0 false 1 true
#define TYPE_NUMBER 1
// hibits 0 : 0 , 1: byte, 2:word, 4: dword, 6: qword, 8 : double
#define TYPE_NUMBER_ZERO 0
#define TYPE_NUMBER_BYTE 1
#define TYPE_NUMBER_WORD 2
#define TYPE_NUMBER_DWORD 4
#define TYPE_NUMBER_QWORD 6
#define TYPE_NUMBER_REAL 8

#define TYPE_USERDATA 2
// hibits 0 : void *
// hibits 1 : c function
#define TYPE_USERDATA_POINTER 0
#define TYPE_USERDATA_CFUNCTION 1

#define TYPE_SHORT_STRING 3
// hibits 0~31 : len
#define TYPE_LONG_STRING 4

// hibits 0~30 : array size , 31 : extend size
#define TYPE_TABLE 5
#define TYPE_TABLE_MARK 6

// hibits 0~30 : ref ancestor id , 31 : extend id ( number )
#define TYPE_REF 7

#define MAX_COOKIE 32
#define EXTEND_NUMBER (MAX_COOKIE-1)
#define COMBINE_TYPE(t,v) ((t) | (v) << 3)

#define BLOCK_SIZE 128
#define MAX_DEPTH 31

#define MAX_REFERENCE 32

struct block {
	struct block * next;
	char buffer[BLOCK_SIZE];
};

struct stack {
	int depth;
	int ref_index;
	int objectid;
	int ancestor[MAX_DEPTH];
};

struct reference {
	const void * object;
	uint8_t * address;
};

struct write_block {
	struct block * head;
	struct block * current;
	int len;
	int ptr;
	struct stack s;
	struct reference r[MAX_REFERENCE];
};

struct read_block {
	char * buffer;
	int len;
	int ptr;
	struct stack s;
};

inline static struct block *
blk_alloc(void) {
	struct block *b = malloc(sizeof(struct block));
	b->next = NULL;
	return b;
}

static inline void
wb_push(struct write_block *b, const void *buf, int sz) {
	const char * buffer = buf;
	if (b->ptr == BLOCK_SIZE) {
_again:
		b->current = b->current->next = blk_alloc();
		b->ptr = 0;
	}
	if (b->ptr <= BLOCK_SIZE - sz) {
		memcpy(b->current->buffer + b->ptr, buffer, sz);
		b->ptr+=sz;
		b->len+=sz;
	} else {
		int copy = BLOCK_SIZE - b->ptr;
		memcpy(b->current->buffer + b->ptr, buffer, copy);
		buffer += copy;
		b->len += copy;
		sz -= copy;
		goto _again;
	}
}

static inline void *
wb_address(struct write_block *b) {
	if (b->ptr == BLOCK_SIZE) {
		b->current = b->current->next = blk_alloc();
		b->ptr = 0;
	}
	return (void *)(b->current->buffer + b->ptr);
}

static inline void
init_stack(struct stack *s) {
	s->depth = 0;
	s->objectid = 0;
	s->ref_index = 0;
}

static void
wb_init(struct write_block *wb , struct block *b) {
	wb->head = b;
	assert(b->next == NULL);
	wb->len = 0;
	wb->current = wb->head;
	wb->ptr = 0;
	init_stack(&wb->s);
}

static void
wb_free(struct write_block *wb) {
	struct block *blk = wb->head;
	blk = blk->next;	// the first block is on stack
	while (blk) {
		struct block * next = blk->next;
		free(blk);
		blk = next;
	}
	wb->head = NULL;
	wb->current = NULL;
	wb->ptr = 0;
	wb->len = 0;
}

static void
rball_init(struct read_block * rb, char * buffer, int size) {
	rb->buffer = buffer;
	rb->len = size;
	rb->ptr = 0;
	init_stack(&rb->s);
}

static const void *
rb_read(struct read_block *rb, int sz) {
	if (rb->len < sz) {
		return NULL;
	}

	int ptr = rb->ptr;
	rb->ptr += sz;
	rb->len -= sz;
	return rb->buffer + ptr;
}

static inline void
wb_nil(struct write_block *wb) {
	uint8_t n = COMBINE_TYPE(TYPE_BOOLEAN , TYPE_BOOLEAN_NIL);
	wb_push(wb, &n, 1);
}

static inline void
wb_boolean(struct write_block *wb, int boolean) {
	uint8_t n = COMBINE_TYPE(TYPE_BOOLEAN , boolean ? TYPE_BOOLEAN_TRUE : TYPE_BOOLEAN_FALSE);
	wb_push(wb, &n, 1);
}

static inline void
wb_integer(struct write_block *wb, lua_Integer v) {
	int type = TYPE_NUMBER;
	if (v == 0) {
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_ZERO);
		wb_push(wb, &n, 1);
	} else if (v != (int32_t)v) {
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_QWORD);
		int64_t v64 = v;
		wb_push(wb, &n, 1);
		wb_push(wb, &v64, sizeof(v64));
	} else if (v < 0) {
		int32_t v32 = (int32_t)v;
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_DWORD);
		wb_push(wb, &n, 1);
		wb_push(wb, &v32, sizeof(v32));
	} else if (v<0x100) {
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_BYTE);
		wb_push(wb, &n, 1);
		uint8_t byte = (uint8_t)v;
		wb_push(wb, &byte, sizeof(byte));
	} else if (v<0x10000) {
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_WORD);
		wb_push(wb, &n, 1);
		uint16_t word = (uint16_t)v;
		wb_push(wb, &word, sizeof(word));
	} else {
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_DWORD);
		wb_push(wb, &n, 1);
		uint32_t v32 = (uint32_t)v;
		wb_push(wb, &v32, sizeof(v32));
	}
}

static inline void
wb_real(struct write_block *wb, double v) {
	uint8_t n = COMBINE_TYPE(TYPE_NUMBER , TYPE_NUMBER_REAL);
	wb_push(wb, &n, 1);
	wb_push(wb, &v, sizeof(v));
}

static inline void
wb_pointer(struct write_block *wb, void *v, int subtype) {
	uint8_t n = COMBINE_TYPE(TYPE_USERDATA, subtype);
	wb_push(wb, &n, 1);
	wb_push(wb, &v, sizeof(v));
}

static inline void
wb_string(struct write_block *wb, const char *str, int len) {
	if (len < MAX_COOKIE) {
		uint8_t n = COMBINE_TYPE(TYPE_SHORT_STRING, len);
		wb_push(wb, &n, 1);
		if (len > 0) {
			wb_push(wb, str, len);
		}
	} else {
		uint8_t n;
		if (len < 0x10000) {
			n = COMBINE_TYPE(TYPE_LONG_STRING, 2);
			wb_push(wb, &n, 1);
			uint16_t x = (uint16_t) len;
			wb_push(wb, &x, 2);
		} else {
			n = COMBINE_TYPE(TYPE_LONG_STRING, 4);
			wb_push(wb, &n, 1);
			uint32_t x = (uint32_t) len;
			wb_push(wb, &x, 4);
		}
		wb_push(wb, str, len);
	}
}

static void pack_one(lua_State *L, struct write_block *b, int index);

static int
wb_table_array(lua_State *L, struct write_block * wb, int index) {
	int array_size = (int)lua_rawlen(L,index);
	if (array_size >= EXTEND_NUMBER) {
		uint8_t n = COMBINE_TYPE(TYPE_TABLE, EXTEND_NUMBER);
		wb_push(wb, &n, 1);
		wb_integer(wb, array_size);
	} else {
		uint8_t n = COMBINE_TYPE(TYPE_TABLE, array_size);
		wb_push(wb, &n, 1);
	}

	int i;
	for (i=1;i<=array_size;i++) {
		lua_rawgeti(L,index,i);
		pack_one(L, wb, -1);
		lua_pop(L,1);
	}

	return array_size;
}

static void
wb_table_hash(lua_State *L, struct write_block * wb, int index, int array_size) {
	lua_pushnil(L);
	while (lua_next(L, index) != 0) {
		if (lua_type(L,-2) == LUA_TNUMBER) {
			if (lua_isinteger(L, -2)) {
				lua_Integer x = lua_tointeger(L,-2);
				if (x>0 && x<=array_size) {
					lua_pop(L,1);
					continue;
				}
			}
		}
		pack_one(L,wb,-2);
		pack_one(L,wb,-1);
		lua_pop(L, 1);
	}
	wb_nil(wb);
}

static void
wb_table_metapairs(lua_State *L, struct write_block *wb, int index) {
	uint8_t n = COMBINE_TYPE(TYPE_TABLE, 0);
	wb_push(wb, &n, 1);
	lua_pushvalue(L, index);
	lua_call(L, 1, 3);
	for(;;) {
		lua_pushvalue(L, -2);
		lua_pushvalue(L, -2);
		lua_copy(L, -5, -3);
		lua_call(L, 2, 2);
		int type = lua_type(L, -2);
		if (type == LUA_TNIL) {
			lua_pop(L, 4);
			break;
		}
		pack_one(L, wb, -2);
		pack_one(L, wb, -1);
		lua_pop(L, 1);
	}
	wb_nil(wb);
}

static inline void
mark_table(lua_State *L, struct write_block *b, int index) {
	const void * obj = lua_topointer(L, index);
	struct stack *s = &b->s;
	int id = s->objectid++;
	void *addr = wb_address(b);
	if (id == MAX_REFERENCE) {
		lua_createtable(L, 0, MAX_REFERENCE+1);
		lua_replace(L, s->ref_index);
		lua_createtable(L, MAX_REFERENCE+1, 0);
		lua_replace(L, s->ref_index+1);
		int i;
		for (i=0;i<MAX_REFERENCE;i++) {
			lua_pushinteger(L, i+1);
			lua_rawsetp(L, s->ref_index, b->r[i].object);
			if (b->r[i].address) {
				lua_pushlightuserdata(L, (void *)b->r[i].address);
				lua_rawseti(L, s->ref_index+1, i+1);
			}
		}
	}
	if (id < MAX_REFERENCE) {
		b->r[id].object = obj;
		b->r[id].address = addr;
	} else {
		++id;
		lua_pushinteger(L, id);
		lua_rawsetp(L, s->ref_index, obj);
		lua_pushlightuserdata(L, addr);
		lua_rawseti(L, s->ref_index + 1, id);
	}
}

static void
wb_table(lua_State *L, struct write_block *wb, int index) {
	luaL_checkstack(L, LUA_MINSTACK, NULL);
	if (index < 0) {
		index = lua_gettop(L) + index + 1;
	}
	mark_table(L, wb, index);
	if (luaL_getmetafield(L, index, "__pairs") != LUA_TNIL) {
		wb_table_metapairs(L, wb, index);
	} else {
		int array_size = wb_table_array(L, wb, index);
		wb_table_hash(L, wb, index, array_size);
	}
}

static int
ref_ancestor(lua_State *L, struct write_block *b, int index) {
	struct stack *s = &b->s;
	int n = s->depth;
	if (n == 0 || n >= MAX_DEPTH)
		return 0;
	int i;
	const void * obj = lua_topointer(L, index);
	for (i=n-1;i>=0;i--) {
		const void * ancestor = lua_topointer(L, s->ancestor[i]);
		if (ancestor == obj) {
			uint8_t n = COMBINE_TYPE(TYPE_REF, i);
			wb_push(b, &n, 1);
			return 1;
		}
	}
	return 0;
}

static inline void
change_mark(uint8_t *tag) {
	assert((*tag & 0x7) == TYPE_TABLE);
	*tag = COMBINE_TYPE(TYPE_TABLE_MARK, *tag >> 3);
}

static inline int
lookup_ref(lua_State *L, struct write_block *b, const void *obj) {
	if (b->s.objectid <= MAX_REFERENCE) {
		int i;
		for (i=0;i<b->s.objectid;i++) {
			if (obj == b->r[i].object) {
				if (b->r[i].address) {
					change_mark(b->r[i].address);
					b->r[i].address = NULL;
				}
				return i+1;
			}
		}
		return 0;
	} else {
		if (lua_rawgetp(L, b->s.ref_index, obj) != LUA_TNUMBER) {
			lua_pop(L, 1);
			return 0;
		}
		int id = lua_tointeger(L, -1);
		lua_pop(L, 1);
		if (lua_rawgeti(L, b->s.ref_index + 1, id) == LUA_TLIGHTUSERDATA) {
			uint8_t * tag = (uint8_t *)lua_touserdata(L, -1);
			lua_pop(L, 1);
			change_mark(tag);
			lua_pushnil(L);
			lua_rawseti(L, b->s.ref_index + 1, id);
		} else {
			lua_pop(L, 1);
		}
		return id;
	}
}

static int
ref_object(lua_State *L, struct write_block *b, int index) {
	const void * obj = lua_topointer(L, index);
	int id = lookup_ref(L, b, obj);
	if (id > 0) {
		uint8_t n = COMBINE_TYPE(TYPE_REF, EXTEND_NUMBER);
		wb_push(b, &n, 1);
		wb_integer(b, id);
		return 1;
	} else {
		return 0;
	}
}

static void
pack_one(lua_State *L, struct write_block *b, int index) {
	struct stack *s = &b->s;
	int type = lua_type(L,index);
	switch(type) {
	case LUA_TNIL:
		wb_nil(b);
		break;
	case LUA_TNUMBER: {
		if (lua_isinteger(L, index)) {
			lua_Integer x = lua_tointeger(L,index);
			wb_integer(b, x);
		} else {
			lua_Number n = lua_tonumber(L,index);
			wb_real(b,n);
		}
		break;
	}
	case LUA_TBOOLEAN: 
		wb_boolean(b, lua_toboolean(L,index));
		break;
	case LUA_TSTRING: {
		size_t sz = 0;
		const char *str = lua_tolstring(L,index,&sz);
		wb_string(b, str, (int)sz);
		break;
	}
	case LUA_TLIGHTUSERDATA:
		wb_pointer(b, lua_touserdata(L,index), TYPE_USERDATA_POINTER);
		break;
	case LUA_TFUNCTION: {
		lua_CFunction func = lua_tocfunction(L,index);
		if (func == NULL || lua_getupvalue(L, index, 1) != NULL) {
			luaL_error(L, "Only light C function can be serialized");
		}
		wb_pointer(b, (void *)func, TYPE_USERDATA_CFUNCTION);
		break; }
	case LUA_TTABLE: {
		if (index < 0) {
			index = lua_gettop(L) + index + 1;
		}
		if (ref_ancestor(L, b, index))
			break;
		if (ref_object(L, b, index))
			break;
		if (s->depth < MAX_DEPTH)
			s->ancestor[s->depth] = index;
		++s->depth;
		wb_table(L, b, index);
		--s->depth;
		break;
	}
	default:
		wb_free(b);
		luaL_error(L, "Unsupport type %s to serialize", lua_typename(L, type));
	}
}

static void
pack_from(lua_State *L, struct write_block *b, int from) {
	int top = lua_gettop(L);
	int n = top - from;
	int i;
	lua_pushnil(L);	// slot for table ref lookup { pointer -> id }
	lua_pushnil(L);	// slot for table refs array { address, ... }
	b->s.ref_index = top + 1;
	for (i=1;i<=n;i++) {
		pack_one(L, b , from + i);
	}
}

static inline void
invalid_stream_line(lua_State *L, struct read_block *rb, int line) {
	int len = rb->len;
	luaL_error(L, "Invalid serialize stream %d (line:%d)", len, line);
}

#define invalid_stream(L,rb) invalid_stream_line(L,rb,__LINE__)

static lua_Integer
get_integer(lua_State *L, struct read_block *rb, int cookie) {
	switch (cookie) {
	case TYPE_NUMBER_ZERO:
		return 0;
	case TYPE_NUMBER_BYTE: {
		uint8_t n;
		const uint8_t * pn = (const uint8_t *)rb_read(rb,sizeof(n));
		if (pn == NULL)
			invalid_stream(L,rb);
		n = *pn;
		return n;
	}
	case TYPE_NUMBER_WORD: {
		uint16_t n;
		const void * pn = rb_read(rb,sizeof(n));
		if (pn == NULL)
			invalid_stream(L,rb);
		memcpy(&n, pn, sizeof(n));
		return n;
	}
	case TYPE_NUMBER_DWORD: {
		int32_t n;
		const void * pn = rb_read(rb,sizeof(n));
		if (pn == NULL)
			invalid_stream(L,rb);
		memcpy(&n, pn, sizeof(n));
		return n;
	}
	case TYPE_NUMBER_QWORD: {
		int64_t n;
		const void * pn = rb_read(rb,sizeof(n));
		if (pn == NULL)
			invalid_stream(L,rb);
		memcpy(&n, pn, sizeof(n));
		return n;
	}
	default:
		invalid_stream(L,rb);
		return 0;
	}
}

static double
get_real(lua_State *L, struct read_block *rb) {
	double n;
	const void * pn = rb_read(rb,sizeof(n));
	if (pn == NULL)
		invalid_stream(L,rb);
	memcpy(&n, pn, sizeof(n));
	return n;
}

static void *
get_pointer(lua_State *L, struct read_block *rb) {
	void * userdata = 0;
	const void * v = rb_read(rb,sizeof(userdata));
	if (v == NULL) {
		invalid_stream(L,rb);
	}
	memcpy(&userdata, v, sizeof(userdata));
	return userdata;
}

static void
get_buffer(lua_State *L, struct read_block *rb, int len) {
	const char * p = (const char *)rb_read(rb,len);
	if (p == NULL) {
		invalid_stream(L,rb);
	}
	lua_pushlstring(L,p,len);
}

static void unpack_one(lua_State *L, struct read_block *rb);

static int
get_extend_integer(lua_State *L, struct read_block *rb) {
	uint8_t type;
	const uint8_t *t = rb_read(rb, sizeof(type));
	if (t==NULL) {
		invalid_stream(L,rb);
	}
	type = *t;
	int cookie = type >> 3;
	if ((type & 7) != TYPE_NUMBER || cookie == TYPE_NUMBER_REAL) {
		invalid_stream(L,rb);
	}
	return (int)get_integer(L,rb,cookie);
}

static void
unpack_table(lua_State *L, struct read_block *rb, int array_size, int type) {
	if (array_size == EXTEND_NUMBER) {
		array_size = get_extend_integer(L, rb);
	}
	struct stack *s = &rb->s;
	int id = ++s->objectid;
	luaL_checkstack(L,LUA_MINSTACK,NULL);
	lua_createtable(L,array_size,0);
	if (type == TYPE_TABLE_MARK) {
		lua_pushvalue(L, -1);
		if (lua_type(L, s->ref_index) == LUA_TNIL) {
			lua_newtable(L);
			lua_replace(L, s->ref_index);
		}
		lua_rawseti(L, s->ref_index, id);
	}
	if (s->depth < MAX_DEPTH)
		s->ancestor[s->depth] = lua_gettop(L);
	++s->depth;
	int i;
	for (i=1;i<=array_size;i++) {
		unpack_one(L,rb);
		lua_rawseti(L,-2,i);
	}
	--s->depth;
	for (;;) {
		unpack_one(L,rb);
		if (lua_isnil(L,-1)) {
			lua_pop(L,1);
			return;
		}
		++s->depth;
		unpack_one(L,rb);
		--s->depth;
		lua_rawset(L,-3);
	}
}

static void
unpack_ref(lua_State *L, struct read_block *rb, int ref) {
	struct stack *s = &rb->s;
	if (ref == EXTEND_NUMBER) {
		int id = get_extend_integer(L, rb);
		if (lua_type(L, s->ref_index) != LUA_TTABLE || lua_rawgeti(L, s->ref_index, id) != LUA_TTABLE) {
			luaL_error(L, "Invalid ref object id %d", id);
		}
	} else {
		if (ref >= s->depth)
			luaL_error(L, "Invalid ref object %d/%d", ref, s->depth);
		lua_pushvalue(L, s->ancestor[ref]);
	}
}

static void
push_value(lua_State *L, struct read_block *rb, int type, int cookie) {
	switch(type) {
	case TYPE_BOOLEAN:
		switch (cookie) {
		case TYPE_BOOLEAN_NIL:
			lua_pushnil(L);
			break;
		case TYPE_BOOLEAN_FALSE:
			lua_pushboolean(L,0);
			break;
		case TYPE_BOOLEAN_TRUE:
			lua_pushboolean(L,1);
			break;
		default:
			luaL_error(L, "Invalid boolean subtype %d", cookie);
		}
		break;
	case TYPE_NUMBER:
		if (cookie == TYPE_NUMBER_REAL) {
			lua_pushnumber(L,get_real(L,rb));
		} else {
			lua_pushinteger(L, get_integer(L, rb, cookie));
		}
		break;
	case TYPE_USERDATA:
		if (cookie == TYPE_USERDATA_POINTER)
			lua_pushlightuserdata(L,get_pointer(L,rb));
		else {
			if (cookie != TYPE_USERDATA_CFUNCTION)
				luaL_error(L, "Invalid userdata");
			lua_pushcfunction(L, (lua_CFunction)get_pointer(L, rb));
		}
		break;
	case TYPE_SHORT_STRING:
		get_buffer(L,rb,cookie);
		break;
	case TYPE_LONG_STRING: {
		if (cookie == 2) {
			const void *plen = rb_read(rb, 2);
			if (plen == NULL) {
				invalid_stream(L,rb);
			}
			uint16_t n;
			memcpy(&n, plen, sizeof(n));
			get_buffer(L,rb,n);
		} else {
			if (cookie != 4) {
				invalid_stream(L,rb);
			}
			const void *plen = rb_read(rb, 4);
			if (plen == NULL) {
				invalid_stream(L,rb);
			}
			uint32_t n;
			memcpy(&n, plen, sizeof(n));
			get_buffer(L,rb,n);
		}
		break;
	}
	case TYPE_TABLE:
	case TYPE_TABLE_MARK:
		unpack_table(L,rb,cookie,type);
		break;
	case TYPE_REF:
		unpack_ref(L,rb,cookie);
		break;
	default:
		invalid_stream(L,rb);
		break;
	}
}

static void
unpack_one(lua_State *L, struct read_block *rb) {
	uint8_t type;
	const uint8_t *t = rb_read(rb, sizeof(type));
	if (t==NULL) {
		invalid_stream(L, rb);
	}
	type = *t;
	push_value(L, rb, type & 0x7, type>>3);
}

static void *
seri(struct block *b, int len) {
	uint8_t * buffer = malloc(len + 4);
	memcpy(buffer, &len, 4);	// write length
	uint8_t * ptr = buffer + 4;
	while(len>0) {
		if (len >= BLOCK_SIZE) {
			memcpy(ptr, b->buffer, BLOCK_SIZE);
			ptr += BLOCK_SIZE;
			len -= BLOCK_SIZE;
			b = b->next;
		} else {
			memcpy(ptr, b->buffer, len);
			break;
		}
	}

	return buffer;
}

int
seri_unpack(lua_State *L, void *buffer) {
	int top = lua_gettop(L);
	int len = 0;
	memcpy(&len, buffer, 4);	// get length

	struct read_block rb;
	rball_init(&rb, (char *)buffer + 4, len);
	lua_pushnil(L);	// slot for ref table
	rb.s.ref_index = top + 1;

	int i;
	for (i=0;;i++) {
		if (i%8==0) {
			luaL_checkstack(L,LUA_MINSTACK,NULL);
		}
		uint8_t type = 0;
		const uint8_t *t = rb_read(&rb, sizeof(type));
		if (t==NULL)
			break;
		type = *t;
		push_value(L, &rb, type & 0x7, type>>3);
	}

	return lua_gettop(L) - 1 - top;
}

static int
seri_unpack_(lua_State *L) {
	void *buffer = lua_touserdata(L, 1);
	lua_settop(L, 0);
	return seri_unpack(L, buffer);
}

int
seri_unpackptr(lua_State *L, void *buffer) {
	int top = lua_gettop(L);
	lua_pushcfunction(L, seri_unpack_);
	lua_pushlightuserdata(L, buffer);
	int err = lua_pcall(L, 1, LUA_MULTRET, 0);
	free(buffer);
	if (err != LUA_OK) {
		lua_error(L);
	}
	return lua_gettop(L) - top;
}

int
seri_unpackstr(lua_State *L) {
	const char * buffer = luaL_checkstring(L, 1);
	lua_settop(L, 1);
	lua_pushcfunction(L, seri_unpack_);
	lua_pushlightuserdata(L, (void *)buffer);
	if (lua_pcall(L, 1, LUA_MULTRET, 0) != LUA_OK) {
		lua_error(L);
	}
	return lua_gettop(L) - 1;
}

void *
seri_pack(lua_State *L, int from, int *sz) {
	struct block temp;
	temp.next = NULL;
	struct write_block wb;
	wb_init(&wb, &temp);

	pack_from(L,&wb,from);
	assert(wb.head == &temp);

	void * buffer = seri(&temp, wb.len);

	if (sz) {
		*sz = wb.len + 4;
	}

	wb_free(&wb);

	return buffer;
}

void *
seri_packstring(const char * str, int sz, void *p, size_t *output) {
	struct block temp;
	temp.next = NULL;
	struct write_block wb;
	wb_init(&wb, &temp);

	if (sz == 0) {
		sz = strlen(str);
	}
	wb_string(&wb, str, sz);
	if (p) {
		wb_pointer(&wb, p, TYPE_USERDATA_POINTER);
	}
	assert(wb.head == &temp);

	void * buffer = seri(&temp, wb.len);
	if (output) {
		*output = wb.len + 4;
	}

	wb_free(&wb);

	return buffer;
}

int
luaseri_unpack(lua_State *L) {
	if (lua_isnoneornil(L, 1)) {
		return 0;
	}
	lua_settop(L, 1);
	return seri_unpack_(L);
}

int
luaseri_unpack_remove(lua_State *L) {
	if (lua_isnoneornil(L, 1)) {
		return 0;
	}
	return seri_unpackptr(L, lua_touserdata(L, 1));
}

int
luaseri_pack(lua_State *L) {
	int sz = 0;
	void * buffer = seri_pack(L, 0, &sz);
	lua_pushlightuserdata(L, buffer);
	lua_pushinteger(L, sz);
	return 2;
}

int
luaseri_remove(lua_State *L) {
	if (lua_isnoneornil(L, 1))
		return 0;
	luaL_checktype(L, 1, LUA_TLIGHTUSERDATA);
	void * data = lua_touserdata(L, 1);
	size_t sz = luaL_checkinteger(L, 2);
	(void)sz;
	free(data);
	return 0;
}

#ifdef TEST_SERI

LUAMOD_API int
luaopen_seri(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "pack", luaseri_pack },
		{ "unpack", luaseri_unpack },
		{ "unpack_remove", luaseri_unpack_remove },
		{ NULL, NULL },
	};
	luaL_newlib(L, l);
	return 1;
}

#endif
