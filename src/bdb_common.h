#pragma once

#include <endian.h>
#include <string.h>
#include <stdint.h>

#define MAX_NAME_LEN 256
#define SIZE_U32 (sizeof(uint32_t))
#define SIZE_REC (sizeof(db_recno_t))

typedef uint32_t bufoff_t;
typedef uint32_t bufsize_t;

static inline void buffer_set_rec(
		char *buffer,
		bufoff_t offset,
		db_recno_t value)
{ /* TODO recno requires endian conversion? */
	memcpy(buffer + offset, &value, SIZE_REC);
}

static inline db_recno_t buffer_get_rec(
		const char *buffer,
		bufoff_t offset)
{
	db_recno_t value;
	memcpy(&value, buffer + offset, SIZE_REC);
	return value;
}

static inline void buffer_set_u32(
		char *buffer,
		bufoff_t offset,
		uint32_t value)
{
	value = htole32(value);
	memcpy(buffer + offset, &value, SIZE_U32);
}

static inline void buffer_add_u32(
		char *buffer,
		bufoff_t offset,
		int32_t value)
{
	uint32_t tmp;
	memcpy(&tmp, buffer + offset, SIZE_U32);
	tmp = htole32(le32toh(tmp) + value);
	memcpy(buffer + offset, &tmp, SIZE_U32);
}

static inline uint32_t buffer_get_u32(
		const char *buffer,
		bufoff_t offset)
{
	uint32_t value;
	memcpy(&value, buffer + offset, SIZE_U32);
	return le32toh(value);
}

static inline void buffer_set_data(
		char *buffer,
		bufoff_t offset,
		const void *data,
		bufsize_t size)
{
	memcpy(buffer + offset, data, size);
}

static inline void buffer_get_data(
		const char *buffer,
		bufoff_t offset,
		void *data,
		bufsize_t size)
{
	memcpy(data, buffer + offset, size);
}

static inline void buffer_move_internal(
		char *buffer,
		bufoff_t doff,
		bufoff_t soff,
		bufsize_t size)
{
	memmove(buffer + doff, buffer + soff, size);
}

static inline void buffer_move_external(
		char *di,
		bufoff_t doff,
		const void *si,
		bufoff_t soff,
		bufsize_t size)
{
	memcpy(di + doff, si + soff, size);
}

static inline void buffer_fill(
		char *buffer,
		bufoff_t offset,
		char value,
		bufsize_t size)
{
	memset(buffer + offset, value, size);
}

