// Copyright 2015-2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "natsp.h"

#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>

#include "util.h"
#include "mem.h"

int jsonMaxNested = JSON_MAX_NEXTED;

// Forward declarations due to recursive calls
static natsStatus _jsonParse(nats_JSON **newJSON, int *parsedLen, const char *jsonStr, int jsonLen, int nested);
static natsStatus _jsonParseValue(char **str, nats_JSONField *field, int nested);
static void       _jsonFreeArray(nats_JSONArray *arr, bool freeObj);

#define JSON_GET_AS(jt, t) \
natsStatus      s      = NATS_OK;                       \
nats_JSONField  *field = NULL;                          \
s = nats_JSONGetField(json, fieldName, (jt), &field);   \
if ((s == NATS_OK) && (field == NULL))                  \
{                                                       \
    *value = 0;                                         \
    return NATS_OK;                                     \
}                                                       \
else if (s == NATS_OK)                                  \
{                                                       \
    switch (field->numTyp)                              \
    {                                                   \
        case TYPE_INT:                                  \
            *value = (t)field->value.vint;  break;      \
        case TYPE_UINT:                                 \
            *value = (t)field->value.vuint; break;      \
        default:                                        \
            *value = (t)field->value.vdec;              \
    }                                                   \
}                                                       \
return NATS_UPDATE_ERR_STACK(s);

#define JSON_ARRAY_AS(t) \
int i;                                              \
t* values = (t*) NATS_CALLOC(arr->size, sizeof(t)); \
if (values == NULL)                                 \
    return nats_setDefaultError(NATS_NO_MEMORY);    \
for (i=0; i<arr->size; i++)                         \
    values[i] = ((t*) arr->values)[i];              \
*array     = values;                                \
*arraySize = arr->size;                             \
return NATS_OK;

#define JSON_ARRAY_AS_NUM(t) \
int i;                                                      \
t* values = (t*) NATS_CALLOC(arr->size, sizeof(t));         \
if (values == NULL)                                         \
    return nats_setDefaultError(NATS_NO_MEMORY);            \
for (i=0; i<arr->size; i++)                                 \
{                                                           \
    void *ptr = NULL;                                       \
    ptr = (void*) ((char*)(arr->values)+(i*jsonMaxNumSize));\
    values[i] = *(t*) ptr;                                  \
}                                                           \
*array     = values;                                        \
*arraySize = arr->size;                                     \
return NATS_OK;

#define JSON_GET_ARRAY(t, f) \
natsStatus      s      = NATS_OK;                           \
nats_JSONField  *field = NULL;                              \
s = nats_JSONGetArrayField(json, fieldName, (t), &field);   \
if ((s == NATS_OK) && (field == NULL))                      \
{                                                           \
    *array      = NULL;                                     \
    *arraySize  = 0;                                        \
    return NATS_OK;                                         \
}                                                           \
else if (s == NATS_OK)                                      \
    s = (f)(field->value.varr, array, arraySize);           \
return NATS_UPDATE_ERR_STACK(s);


#define ASCII_0 (48)
#define ASCII_9 (57)

static char base32DecodeMap[256];

static const char *base64EncodeURL= "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

// An implementation of crc16 according to CCITT standards for XMODEM.
static uint16_t crc16tab[256] = {
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
};

// parseInt64 expects decimal positive numbers. We
// return -1 to signal error
int64_t
nats_ParseInt64(const char *d, int dLen)
{
    int     i;
    char    dec;
    int64_t pn = 0;
    int64_t n  = 0;

    if (dLen == 0)
        return -1;

    for (i=0; i<dLen; i++)
    {
        dec = d[i];
        if ((dec < ASCII_0) || (dec > ASCII_9))
            return -1;

        n = (n * 10) + (int64_t)(dec - ASCII_0);

        // Check overflow..
        if (n < pn)
            return -1;

        pn = n;
    }

    return n;
}

natsStatus
nats_Trim(char **pres, const char *s)
{
    int     len    = 0;
    char    *res   = NULL;
    char    *ptr   = (char*) s;
    char    *start = (char*) s;

    while ((*ptr != '\0') && isspace(*ptr))
        ptr++;

    start = ptr;
    ptr = (char*) (s + strlen(s) - 1);
    while ((ptr != start) && isspace(*ptr))
        ptr--;

    // Compute len of trimmed string
    len = (int) (ptr-start) + 1;

    // Allocate for copy (add 1 for terminating 0)
    res = NATS_MALLOC(len+1);
    if (res == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    memcpy(res, start, (size_t) len);
    res[len] = '\0';
    *pres = res;

    return NATS_OK;
}

natsStatus
nats_ParseControl(natsControl *control, const char *line)
{
    natsStatus  s           = NATS_OK;
    char        *tok        = NULL;
    int         len         = 0;

    if ((line == NULL) || (line[0] == '\0'))
        return nats_setDefaultError(NATS_PROTOCOL_ERROR);

    tok = strchr(line, (int) ' ');
    if (tok == NULL)
    {
        control->op = NATS_STRDUP(line);
        if (control->op == NULL)
            return nats_setDefaultError(NATS_NO_MEMORY);

        return NATS_OK;
    }

    len = (int) (tok - line);
    control->op = NATS_MALLOC(len + 1);
    if (control->op == NULL)
    {
        s = nats_setDefaultError(NATS_NO_MEMORY);
    }
    else
    {
        memcpy(control->op, line, len);
        control->op[len] = '\0';
    }

    if (s == NATS_OK)
    {
        // Discard all spaces and the like in between the next token
        while ((tok[0] != '\0')
               && ((tok[0] == ' ')
                   || (tok[0] == '\r')
                   || (tok[0] == '\n')
                   || (tok[0] == '\t')))
        {
            tok++;
        }
    }

    // If there is a token...
    if (tok[0] != '\0')
    {
        char *tmp;

        len = (int) strlen(tok);
        tmp = &(tok[len - 1]);

        // Remove trailing spaces and the like.
        while ((tmp[0] != '\0')
                && ((tmp[0] == ' ')
                    || (tmp[0] == '\r')
                    || (tmp[0] == '\n')
                    || (tmp[0] == '\t')))
        {
            tmp--;
            len--;
        }

        // We are sure that len is > 0 because of the first while() loop.

        control->args = NATS_MALLOC(len + 1);
        if (control->args == NULL)
        {
            s = nats_setDefaultError(NATS_NO_MEMORY);
        }
        else
        {
            memcpy(control->args, tok, len);
            control->args[len] = '\0';
        }
    }

    if (s != NATS_OK)
    {
        NATS_FREE(control->op);
        control->op = NULL;

        NATS_FREE(control->args);
        control->args = NULL;
    }

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
nats_CreateStringFromBuffer(char **newStr, natsBuffer *buf)
{
    char    *str = NULL;
    int     len  = 0;

    if ((buf == NULL) || ((len = natsBuf_Len(buf)) == 0))
        return NATS_OK;

    str = NATS_MALLOC(len + 1);
    if (str == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    memcpy(str, natsBuf_Data(buf), len);
    str[len] = '\0';

    *newStr = str;

    return NATS_OK;
}

void
nats_Sleep(int64_t millisec)
{
#ifdef _WIN32
    Sleep((DWORD) millisec);
#else
    usleep(millisec * 1000);
#endif
}

const char*
nats_GetBoolStr(bool value)
{
    if (value)
        return "true";

    return "false";
}

void
nats_NormalizeErr(char *error)
{
    int start = 0;
    int end   = 0;
    int len   = (int) strlen(error);
    int i;

    if (strncmp(error, _ERR_OP_, _ERR_OP_LEN_) == 0)
        start = _ERR_OP_LEN_;

    for (i=start; i<len; i++)
    {
        if ((error[i] != ' ') && (error[i] != '\''))
            break;
    }

    start = i;
    if (start == len)
    {
        error[0] = '\0';
        return;
    }

    for (end=len-1; end>0; end--)
    {
        char c = error[end];
        if ((c == '\r') || (c == '\n') || (c == '\'') || (c == ' '))
            continue;
        break;
    }

    if (end <= start)
    {
        error[0] = '\0';
        return;
    }

    len = end - start + 1;
    memmove(error, error + start, len);
    error[len] = '\0';
}

static natsStatus
_jsonCreateField(nats_JSONField **newField, char *fieldName)
{
    nats_JSONField *field = NULL;

    field = (nats_JSONField*) NATS_CALLOC(1, sizeof(nats_JSONField));
    if (field == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    field->name = fieldName;
    field->typ  = TYPE_NOT_SET;

    *newField = field;

    return NATS_OK;
}

static void
_jsonFreeArray(nats_JSONArray *arr, bool freeObj)
{
    if (arr == NULL)
        return;

    if ((arr->typ == TYPE_OBJECT) || (arr->typ == TYPE_ARRAY))
    {
        int i;

        for (i=0; i<arr->size; i++)
        {
            if (arr->typ == TYPE_OBJECT)
            {
                nats_JSON *fjson = ((nats_JSON**)arr->values)[i];
                nats_JSONDestroy(fjson);
            }
            else
            {
                nats_JSONArray *farr = ((nats_JSONArray**)arr->values)[i];
                _jsonFreeArray(farr, true);
            }
        }
    }
    NATS_FREE(arr->values);
    if (freeObj)
        NATS_FREE(arr);
}

static void
_jsonFreeField(nats_JSONField *field)
{
    if (field->typ == TYPE_ARRAY)
        _jsonFreeArray(field->value.varr, true);
    else if (field->typ == TYPE_OBJECT)
        nats_JSONDestroy(field->value.vobj);
    NATS_FREE(field);
}

static char*
_jsonTrimSpace(char *ptr)
{
    while ((*ptr != '\0')
            && ((*ptr == ' ') || (*ptr == '\t') || (*ptr == '\r') || (*ptr == '\n')))
    {
        ptr += 1;
    }
    return ptr;
}

static natsStatus
_jsonGetStr(char **ptr, char **value)
{
    char *p = *ptr;

    while ((*p != '\0') && (*p != '"'))
    {
        if ((*p == '\\') && (*(p + 1) != '\0'))
        {
            p++;
            // based on what http://www.json.org/ says a string should be
            switch (*p)
            {
                case '"':
                case '\\':
                case '/':
                case 'b':
                case 'n':
                case 'r':
                case 't':
                    break;
                case 'u':
                {
                    int i;

                    // Needs to be 4 hex. A hex is a digit or AF, af
                    p++;
                    for (i=0; i<4; i++)
                    {
                        // digit range
                        if (((*p >= '0') && (*p <= '9'))
                                || ((*p >= 'A') && (*p <= 'F'))
                                || ((*p >= 'a') && (*p <= 'f')))
                        {
                            p++;
                        }
                        else
                        {
                            return nats_setError(NATS_ERR,
                                                 "error parsing string '%s': invalid unicode character",
                                                 p);
                        }
                    }
                    p--;
                    break;
                }
                default:
                    return nats_setError(NATS_ERR,
                                         "error parsing string '%s': invalid control character",
                                         p);
            }
        }
        p++;
    }

    if (*p != '\0')
    {
        *value = *ptr;
        *p = '\0';
        *ptr = (char*) (p + 1);
        return NATS_OK;
    }
    return nats_setError(NATS_ERR,
                         "error parsing string '%s': unexpected end of JSON input",
                         *ptr);
}

static natsStatus
_jsonGetNum(char **ptr, nats_JSONField *field)
{
    char        *p             = *ptr;
    bool        expIsNegative  = false;
    uint64_t    uintVal        = 0;
    uint64_t    decVal         = 0;
    uint64_t    decPower       = 1;
    long double sign           = 1.0;
    long double ePower         = 1.0;
    int         decPCount      = 0;
    int         numTyp         = 0;

    while (isspace(*p))
        p++;

    sign = (*p == '-' ? -1.0 : 1.0);

    if ((*p == '-') || (*p == '+'))
        p++;

    while (isdigit(*p))
        uintVal = uintVal * 10 + (*p++ - '0');

    if (*p == '.')
    {
        p++;
        numTyp = TYPE_DOUBLE;
    }

    while (isdigit(*p))
    {
        decVal = decVal * 10 + (*p++ - '0');
        decPower *= 10;
        decPCount++;
    }

    if ((*p == 'e') || (*p == 'E'))
    {
        int64_t eVal = 0;

        numTyp = TYPE_DOUBLE;

        p++;

        expIsNegative = (*p == '-' ? true : false);

        if ((*p == '-') || (*p == '+'))
            p++;

        while (isdigit(*p))
            eVal = eVal * 10 + (*p++ - '0');

        if (expIsNegative)
        {
            if (decPower > 0)
                ePower = (long double) decPower;
        }
        else
        {
            if (decPCount > eVal)
            {
                eVal = decPCount - eVal;
                expIsNegative = true;
            }
            else
            {
                eVal -= decPCount;
            }
        }
        while (eVal != 0)
        {
            ePower *= 10;
            eVal--;
        }
    }

    // If we don't end with a ' ', ',', ']', or '}', this is syntax error.
    if ((*p != ' ') && (*p != ',') && (*p != '}') && (*p != ']'))
        return nats_setError(NATS_ERR,
                             "error parsing number '%s': missing separator or unexpected end of JSON input",
                             *ptr);

    if (numTyp == TYPE_DOUBLE)
    {
        long double res = 0.0;

        if (decVal > 0)
            res = sign * (long double) (uintVal * decPower + decVal);
        else
            res = sign * (long double) uintVal;

        if (ePower > 1)
        {
            if (expIsNegative)
                res /= ePower;
            else
                res *= ePower;
        }
        else if (decVal > 0)
        {
            res /= decPower;
        }
        field->value.vdec = res;
    }
    else if (sign < 0)
    {
        numTyp = TYPE_INT;
        field->value.vint = -((int64_t) uintVal);
    }
    else
    {
        numTyp = TYPE_UINT;
        field->value.vuint = uintVal;
    }
    *ptr = p;
    field->numTyp = numTyp;
    return NATS_OK;
}

static natsStatus
_jsonGetBool(char **ptr, bool *val)
{
    if (strncmp(*ptr, "true", 4) == 0)
    {
        *val = true;
        *ptr += 4;
        return NATS_OK;
    }
    else if (strncmp(*ptr, "false", 5) == 0)
    {
        *val = false;
        *ptr += 5;
        return NATS_OK;
    }
    return nats_setError(NATS_ERR,
                         "error parsing boolean, got: '%s'", *ptr);
}

static natsStatus
_jsonGetArray(char **ptr, nats_JSONArray **newArray, int nested)
{
    natsStatus      s       = NATS_OK;
    char            *p      = *ptr;
    bool            end     = false;
    int             typ     = TYPE_NOT_SET;
    nats_JSONField  field;
    nats_JSONArray  array;

    if (nested >= jsonMaxNested)
        return nats_setError(NATS_ERR, "json reached maximum nested arrays of %d", jsonMaxNested);

    // Initialize our stack variable
    memset(&array, 0, sizeof(nats_JSONArray));

    while ((s == NATS_OK) && (*p != '\0'))
    {
        p = _jsonTrimSpace(p);

        // Initialize the field before parsing.
        memset(&field, 0, sizeof(nats_JSONField));

        s = _jsonParseValue(&p, &field, nested);
        if (s == NATS_OK)
        {
            if (typ == TYPE_NOT_SET)
            {
                typ       = field.typ;
                array.typ = field.typ;

                // Set the element size based on type.
                switch (typ)
                {
                    case TYPE_STR:      array.eltSize = sizeof(char*);              break;
                    case TYPE_BOOL:     array.eltSize = sizeof(bool);               break;
                    case TYPE_NUM:      array.eltSize = jsonMaxNumSize;             break;
                    case TYPE_OBJECT:   array.eltSize = sizeof(nats_JSON*);         break;
                    case TYPE_ARRAY:    array.eltSize = sizeof(nats_JSONArray*);    break;
                    default:
                        s = nats_setError(NATS_ERR,
                                          "array of type %d not supported", typ);
                }
            }
            else if (typ != field.typ)
            {
                s = nats_setError(NATS_ERR,
                                  "array content of different types '%s'",
                                  *ptr);
            }
        }
        if (s != NATS_OK)
            break;

        if (array.size + 1 > array.cap)
        {
            char **newValues  = NULL;
            int newCap      = 2 * array.cap;

            if (newCap == 0)
                newCap = 4;

            newValues = (char**) NATS_REALLOC(array.values, newCap * array.eltSize);
            if (newValues == NULL)
            {
                s = nats_setDefaultError(NATS_NO_MEMORY);
                break;
            }
            array.values = (void**) newValues;
            array.cap    = newCap;
        }
        // Set value based on type
        switch (typ)
        {
            case TYPE_STR:
                ((char**)array.values)[array.size++] = field.value.vstr;
                break;
            case TYPE_BOOL:
                ((bool*)array.values)[array.size++] = field.value.vbool;
                 break;
            case TYPE_NUM:
            {
                void    *ptr = NULL;
                size_t  sz   = 0;

                switch (field.numTyp)
                {
                    case TYPE_INT:
                        ptr = &(field.value.vint);
                        sz  = sizeof(int64_t);
                        break;
                    case TYPE_UINT:
                        ptr = &(field.value.vuint);
                        sz  = sizeof(uint64_t);
                        break;
                    default:
                        ptr = &(field.value.vdec);
                        sz  = sizeof(long double);
                }
                memcpy((void*)(((char *)array.values)+(array.size*array.eltSize)), ptr, sz);
                array.size++;
                break;
            }
            case TYPE_OBJECT:
                ((nats_JSON**)array.values)[array.size++] = field.value.vobj;
                break;
            case TYPE_ARRAY:
                ((nats_JSONArray**)array.values)[array.size++] = field.value.varr;
                break;
        }

        p = _jsonTrimSpace(p);
        if (*p == '\0')
            break;

        if (*p == ']')
        {
            end = true;
            break;
        }
        else if (*p == ',')
        {
            p += 1;
        }
        else
        {
            s = nats_setError(NATS_ERR, "expected ',' got '%s'", p);
        }
    }
    if ((s == NATS_OK) && !end)
    {
        s = nats_setError(NATS_ERR,
                          "unexpected end of array: '%s'",
                          (*p != '\0' ? p : "NULL"));
    }
    if (s == NATS_OK)
    {
        *newArray = NATS_MALLOC(sizeof(nats_JSONArray));
        if (*newArray == NULL)
        {
            s = nats_setDefaultError(NATS_NO_MEMORY);
        }
        else
        {
            memcpy(*newArray, &array, sizeof(nats_JSONArray));
            *ptr = (char*) (p + 1);
        }
    }
    if (s != NATS_OK)
        _jsonFreeArray(&array, false);

    return NATS_UPDATE_ERR_STACK(s);
}

#define JSON_STATE_START        (0)
#define JSON_STATE_NO_FIELD_YET (1)
#define JSON_STATE_FIELD        (2)
#define JSON_STATE_SEPARATOR    (3)
#define JSON_STATE_VALUE        (4)
#define JSON_STATE_NEXT_FIELD   (5)
#define JSON_STATE_END          (6)

static natsStatus
_jsonParseValue(char **str, nats_JSONField *field, int nested)
{
    natsStatus  s    = NATS_OK;
    char        *ptr = *str;

    // Parsing value here. Determine the type based on first character.
    if (*ptr == '"')
    {
        ptr += 1;
        field->typ = TYPE_STR;
        s = _jsonGetStr(&ptr, &field->value.vstr);
    }
    else if ((*ptr == 't') || (*ptr == 'f'))
    {
        field->typ = TYPE_BOOL;
        s = _jsonGetBool(&ptr, &field->value.vbool);
    }
    else if (isdigit(*ptr) || (*ptr == '-'))
    {
        field->typ = TYPE_NUM;
        s = _jsonGetNum(&ptr, field);
    }
    else if (*ptr == '[')
    {
        ptr += 1;
        field->typ = TYPE_ARRAY;
        s = _jsonGetArray(&ptr, &field->value.varr, nested+1);
    }
    else if (*ptr == '{')
    {
        nats_JSON   *object = NULL;
        int         objLen  = 0;

        ptr += 1;
        field->typ = TYPE_OBJECT;
        s = _jsonParse(&object, &objLen, ptr, -1, nested+1);
        if (s == NATS_OK)
        {
            field->value.vobj = object;
            ptr += objLen;
        }
    }
    else
    {
        s = nats_setError(NATS_ERR,
                            "looking for value, got: '%s'", ptr);
    }
    if (s == NATS_OK)
        *str = ptr;

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_jsonParse(nats_JSON **newJSON, int *parsedLen, const char *jsonStr, int jsonLen, int nested)
{
    natsStatus      s         = NATS_OK;
    nats_JSON       *json     = NULL;
    nats_JSONField  *field    = NULL;
    void            *oldField = NULL;
    char            *ptr;
    char            *fieldName = NULL;
    int             state;
    char            *copyStr  = NULL;
    bool            breakLoop = false;

    if (parsedLen != NULL)
        *parsedLen = 0;

    if (nested >= jsonMaxNested)
        return nats_setError(NATS_ERR, "json reached maximum nested objects of %d", jsonMaxNested);

    if (jsonLen < 0)
    {
        if (jsonStr == NULL)
            return nats_setDefaultError(NATS_INVALID_ARG);

        jsonLen = (int) strlen(jsonStr);
    }

    json = NATS_CALLOC(1, sizeof(nats_JSON));
    if (json == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = natsStrHash_Create(&(json->fields), 4);
    if (s == NATS_OK)
    {
        json->str = NATS_MALLOC(jsonLen + 1);
        if (json->str == NULL)
            s = nats_setDefaultError(NATS_NO_MEMORY);

        if (s == NATS_OK)
        {
            memcpy(json->str, jsonStr, jsonLen);
            json->str[jsonLen] = '\0';
        }
    }
    if (s != NATS_OK)
    {
        nats_JSONDestroy(json);
        return NATS_UPDATE_ERR_STACK(s);
    }

    ptr = json->str;
    copyStr = NATS_STRDUP(ptr);
    if (copyStr == NULL)
    {
        nats_JSONDestroy(json);
        return nats_setDefaultError(NATS_NO_MEMORY);
    }
    state = (nested == 0 ? JSON_STATE_START : JSON_STATE_NO_FIELD_YET);

    while ((s == NATS_OK) && (*ptr != '\0') && !breakLoop)
    {
        ptr = _jsonTrimSpace(ptr);
        if (*ptr == '\0')
            break;
        switch (state)
        {
            case JSON_STATE_START:
            {
                // Should be the start of the JSON string
                if (*ptr != '{')
                {
                    s = nats_setError(NATS_ERR, "incorrect JSON string: '%s'", ptr);
                    break;
                }
                ptr += 1;
                state = JSON_STATE_NO_FIELD_YET;
                break;
            }
            case JSON_STATE_NO_FIELD_YET:
            case JSON_STATE_FIELD:
            {
                // Check for end, which is valid only in state == JSON_STATE_NO_FIELD_YET
                if (*ptr == '}')
                {
                    if (state == JSON_STATE_NO_FIELD_YET)
                    {
                        ptr += 1;
                        state = JSON_STATE_END;
                        break;
                    }
                    s = nats_setError(NATS_ERR,
                                      "expected beginning of field, got: '%s'",
                                      ptr);
                    break;
                }
                // Check for
                // Should be the first quote of a field name
                if (*ptr != '"')
                {
                    s = nats_setError(NATS_ERR, "missing quote: '%s'", ptr);
                    break;
                }
                ptr += 1;
                s = _jsonGetStr(&ptr, &fieldName);
                if (s != NATS_OK)
                    break;
                s = _jsonCreateField(&field, fieldName);
                if (s != NATS_OK)
                {
                    NATS_UPDATE_ERR_STACK(s);
                    break;
                }
                s = natsStrHash_Set(json->fields, fieldName, false, (void*) field, &oldField);
                if (s != NATS_OK)
                {
                    NATS_UPDATE_ERR_STACK(s);
                    break;
                }
                if (oldField != NULL)
                {
                    NATS_FREE(oldField);
                    oldField = NULL;
                }
                state = JSON_STATE_SEPARATOR;
                break;
            }
            case JSON_STATE_SEPARATOR:
            {
                // Should be the separation between field name and value.
                if (*ptr != ':')
                {
                    s = nats_setError(NATS_ERR, "missing value for field '%s': '%s'", fieldName, ptr);
                    break;
                }
                ptr += 1;
                state = JSON_STATE_VALUE;
                break;
            }
            case JSON_STATE_VALUE:
            {
                s = _jsonParseValue(&ptr, field, nested);
                if (s == NATS_OK)
                    state = JSON_STATE_NEXT_FIELD;
                break;
            }
            case JSON_STATE_NEXT_FIELD:
            {
                // We should have a ',' separator or be at the end of the string
                if ((*ptr != ',') && (*ptr != '}'))
                {
                    s =  nats_setError(NATS_ERR, "missing separator: '%s' (%s)", ptr, copyStr);
                    break;
                }
                if (*ptr == ',')
                    state = JSON_STATE_FIELD;
                else
                    state = JSON_STATE_END;
                ptr += 1;
                break;
            }
            case JSON_STATE_END:
            {
                if (nested > 0)
                {
                    breakLoop = true;
                    break;
                }
                // If we are here it means that there was a character after the '}'
                // so that's considered a failure.
                s = nats_setError(NATS_ERR,
                                  "invalid characters after end of JSON: '%s'",
                                  ptr);
                break;
            }
        }
    }
    if (s == NATS_OK)
    {
        if (state != JSON_STATE_END)
            s = nats_setError(NATS_ERR, "%s", "JSON string not properly closed");
    }
    if (s == NATS_OK)
    {
        if (parsedLen != NULL)
            *parsedLen = (int) (ptr - json->str);
        *newJSON = json;
    }
    else
        nats_JSONDestroy(json);

    NATS_FREE(copyStr);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
nats_JSONParse(nats_JSON **newJSON, const char *jsonStr, int jsonLen)
{
    natsStatus s = _jsonParse(newJSON, NULL, jsonStr, jsonLen, 0);
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
nats_JSONGetField(nats_JSON *json, const char *fieldName, int fieldType, nats_JSONField **retField)
{
    nats_JSONField *field = NULL;

    field = (nats_JSONField*) natsStrHash_Get(json->fields, (char*) fieldName);
    if (field == NULL)
    {
        *retField = NULL;
        return NATS_OK;
    }

    // Check parsed type matches what is being asked.
    switch (fieldType)
    {
        case TYPE_INT:
        case TYPE_UINT:
        case TYPE_DOUBLE:
            if (field->typ != TYPE_NUM)
                return nats_setError(NATS_INVALID_ARG,
                                     "Asked for field '%s' as type %d, but got type %d when parsing",
                                     field->name, fieldType, field->typ);
            break;
        case TYPE_BOOL:
        case TYPE_STR:
        case TYPE_OBJECT:
            if (field->typ != fieldType)
                return nats_setError(NATS_INVALID_ARG,
                                     "Asked for field '%s' as type %d, but got type %d when parsing",
                                     field->name, fieldType, field->typ);
            break;
        default:
            return nats_setError(NATS_INVALID_ARG,
                                 "Asked for field '%s' as type %d, but this type does not exist",
                                 field->name, fieldType);
    }
    *retField = field;
    return NATS_OK;
}

natsStatus
nats_JSONGetStr(nats_JSON *json, const char *fieldName, char **value)
{
    natsStatus      s      = NATS_OK;
    nats_JSONField  *field = NULL;

    s = nats_JSONGetField(json, fieldName, TYPE_STR, &field);
    if (s == NATS_OK)
    {
        if ((field == NULL) || (field->value.vstr == NULL))
        {
            *value = NULL;
            return NATS_OK;
        }
        else
        {
            char *tmp = NATS_STRDUP(field->value.vstr);
            if (tmp == NULL)
                return nats_setDefaultError(NATS_NO_MEMORY);
            *value = tmp;
        }
    }
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
nats_JSONGetInt(nats_JSON *json, const char *fieldName, int *value)
{
    JSON_GET_AS(TYPE_INT, int);
}

natsStatus
nats_JSONGetInt32(nats_JSON *json, const char *fieldName, int32_t *value)
{
    JSON_GET_AS(TYPE_INT, int32_t);
}

natsStatus
nats_JSONGetUInt16(nats_JSON *json, const char *fieldName, uint16_t *value)
{
    JSON_GET_AS(TYPE_UINT, uint16_t);
}

natsStatus
nats_JSONGetBool(nats_JSON *json, const char *fieldName, bool *value)
{
    natsStatus      s      = NATS_OK;
    nats_JSONField  *field = NULL;

    s = nats_JSONGetField(json, fieldName, TYPE_BOOL, &field);
    if (s == NATS_OK)
    {
        *value = (field == NULL ? false : field->value.vbool);
        return NATS_OK;
    }
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
nats_JSONGetLong(nats_JSON *json, const char *fieldName, int64_t *value)
{
    JSON_GET_AS(TYPE_INT, int64_t);
}

natsStatus
nats_JSONGetULong(nats_JSON *json, const char *fieldName, uint64_t *value)
{
    JSON_GET_AS(TYPE_UINT, uint64_t);
}

natsStatus
nats_JSONGetDouble(nats_JSON *json, const char *fieldName, long double *value)
{
    JSON_GET_AS(TYPE_DOUBLE, long double);
}

natsStatus
nats_JSONGetObject(nats_JSON *json, const char *fieldName, nats_JSON **value)
{
    natsStatus      s      = NATS_OK;
    nats_JSONField  *field = NULL;

    s = nats_JSONGetField(json, fieldName, TYPE_OBJECT, &field);
    if (s == NATS_OK)
    {
        *value = (field == NULL ? NULL : field->value.vobj);
        return NATS_OK;
    }
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
nats_JSONGetTime(nats_JSON *json, const char *fieldName, int64_t *timeUTC)
{
    natsStatus  s           = NATS_OK;
    char        *str        = NULL;
    char        *dotPos     = NULL;
    char        utcOff[7]   = {'\0'};
    int64_t     nanosecs    = 0;
    char        *p          = NULL;
    char        orgStr[35]  = {'\0'};
    char        timeStr[35] = {'\0'};
    char        offSign     = '+';
    int         offHours    = 0;
    int         offMin      = 0;
    int         i, l;
    struct tm   tp;

    s = nats_JSONGetStr(json, fieldName, &str);
    if ((s == NATS_OK) && (str == NULL))
    {
        *timeUTC = 0;
        return NATS_OK;
    }
    else if (s != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    // Check for "0"
    if (strcmp(str, "0001-01-01T00:00:00Z") == 0)
    {
        *timeUTC = 0;
        goto END;
    }

    l = (int) strlen(str);
    // The smallest date/time should be: "YYYY:MM:DDTHH:MM:SSZ", which is 20
    // while the longest should be: "YYYY:MM:DDTHH:MM:SS.123456789-12:34" which is 35
    if ((l < 20) || (l > (int) sizeof(timeStr)))
    {
        if (l < 20)
            s = nats_setError(NATS_INVALID_ARG, "time '%s' too small", str);
        else
            s = nats_setError(NATS_INVALID_ARG, "time '%s' too long", str);
        goto END;
    }

    snprintf(orgStr, sizeof(orgStr), "%s", str);
    memset(&tp, 0, sizeof(struct tm));

    // If ends with 'Z', the time is already UTC
    if ((str[l-1] == 'Z') || (str[l-1] == 'z'))
    {
        // Set the timezone to "+00:00"
        snprintf(utcOff, sizeof(utcOff), "%s", "+00:00");
        str[l-1] = '\0';
    }
    else
    {
        // Make sure the UTC offset comes as "+12:34" (or "-12:34").
        p = str+l-6;
        if ((strlen(p) != 6) || ((*p != '+') && (*p != '-')) || (*(p+3) != ':'))
        {
            s = nats_setError(NATS_INVALID_ARG, "time '%s' has invalid UTC offset", orgStr);
            goto END;
        }
        snprintf(utcOff, sizeof(utcOff), "%s", p);
        // Set end of 'str' to beginning of the offset.
        *p = '\0';
    }

    // Check if there is below seconds precision
    dotPos = strstr(str, ".");
    if (dotPos != NULL)
    {
        int64_t val = 0;

        p = (char*) (dotPos+1);
        // Need to recompute the length, since it has changed.
        l = (int) strlen(p);

        val = nats_ParseInt64((const char*) p, l);
        if (val == -1)
        {
            s = nats_setError(NATS_INVALID_ARG, "time '%s' is invalid", orgStr);
            goto END;
        }

        for (i=0; i<9-l; i++)
            val *= 10;

        if (val > 999999999)
        {
            s = nats_setError(NATS_INVALID_ARG, "time '%s' second fraction too big", orgStr);
            goto END;
        }

        nanosecs = val;
        // Set end of string at the place of the '.'
        *dotPos = '\0';
    }

    snprintf(timeStr, sizeof(timeStr), "%s%s", str, utcOff);
    if (sscanf(timeStr, "%4d-%2d-%2dT%2d:%2d:%2d%c%2d:%2d",
               &tp.tm_year, &tp.tm_mon, &tp.tm_mday, &tp.tm_hour, &tp.tm_min, &tp.tm_sec,
               &offSign, &offHours, &offMin) == 9)
    {
        int64_t res = 0;
        int64_t off = 0;

        tp.tm_year -= 1900;
        tp.tm_mon--;
        tp.tm_isdst = 0;
#ifdef _WIN32
        res = (int64_t) _mkgmtime64(&tp);
#else
        res = (int64_t) timegm(&tp);
#endif
        if (res == -1)
        {
            s = nats_setError(NATS_ERR, "error parsing time '%s'", orgStr);
            goto END;
        }
        // Compute the offset
        off = (int64_t) ((offHours * 60 * 60) + (offMin * 60));
        // If UTC offset is positive, then we need to remove to get down to UTC time,
        // where as if negative, we need to add the offset to get up to UTC time.
        if (offSign == '+')
            off *= (int64_t) -1;

        res *= (int64_t) 1E9;
        res += (off * (int64_t) 1E9);
        res += nanosecs;
        *timeUTC = res;
    }
    else
    {
        s = nats_setError(NATS_ERR, "error parsing time '%s'", orgStr);
    }
END:
    NATS_FREE(str);
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
nats_JSONGetArrayField(nats_JSON *json, const char *fieldName, int fieldType, nats_JSONField **retField)
{
    nats_JSONField  *field   = NULL;

    field = (nats_JSONField*) natsStrHash_Get(json->fields, (char*) fieldName);
    if (field == NULL)
    {
        *retField = NULL;
        return NATS_OK;
    }

    // Check parsed type matches what is being asked.
    if (field->typ != TYPE_ARRAY)
        return nats_setError(NATS_INVALID_ARG,
                             "Field '%s' is not an array, it has type: %d",
                             field->name, field->typ);
    if (fieldType != field->value.varr->typ)
        return nats_setError(NATS_INVALID_ARG,
                             "Asked for field '%s' as an array of type: %d, but it is an array of type: %d",
                             field->name, fieldType, field->typ);

    *retField = field;
    return NATS_OK;
}

natsStatus
nats_JSONArrayAsStrings(nats_JSONArray *arr, char ***array, int *arraySize)
{
    natsStatus  s = NATS_OK;
    int         i;

    char **values = NATS_CALLOC(arr->size, arr->eltSize);
    if (values == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    for (i=0; i<arr->size; i++)
    {
        values[i] = NATS_STRDUP((char*)(arr->values[i]));
        if (values[i] == NULL)
        {
            s = nats_setDefaultError(NATS_NO_MEMORY);
            break;
        }
    }
    if (s != NATS_OK)
    {
        int j;

        for (j=0; j<i; j++)
            NATS_FREE(values[i]);

        NATS_FREE(values);
    }
    else
    {
        *array     = values;
        *arraySize = arr->size;
    }
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
nats_JSONGetArrayStr(nats_JSON *json, const char *fieldName, char ***array, int *arraySize)
{
    JSON_GET_ARRAY(TYPE_STR, nats_JSONArrayAsStrings);
}

natsStatus
nats_JSONArrayAsBools(nats_JSONArray *arr, bool **array, int *arraySize)
{
    JSON_ARRAY_AS(bool);
}

natsStatus
nats_JSONGetArrayBool(nats_JSON *json, const char *fieldName, bool **array, int *arraySize)
{
    JSON_GET_ARRAY(TYPE_BOOL, nats_JSONArrayAsBools);
}

natsStatus
nats_JSONArrayAsDoubles(nats_JSONArray *arr, long double **array, int *arraySize)
{
    JSON_ARRAY_AS_NUM(long double);
}

natsStatus
nats_JSONGetArrayDouble(nats_JSON *json, const char *fieldName, long double **array, int *arraySize)
{
    JSON_GET_ARRAY(TYPE_NUM, nats_JSONArrayAsDoubles);
}

natsStatus
nats_JSONArrayAsInts(nats_JSONArray *arr, int **array, int *arraySize)
{
    JSON_ARRAY_AS_NUM(int);
}

natsStatus
nats_JSONGetArrayInt(nats_JSON *json, const char *fieldName, int **array, int *arraySize)
{
    JSON_GET_ARRAY(TYPE_NUM, nats_JSONArrayAsInts);
}

natsStatus
nats_JSONArrayAsLongs(nats_JSONArray *arr, int64_t **array, int *arraySize)
{
    JSON_ARRAY_AS_NUM(int64_t);
}

natsStatus
nats_JSONGetArrayLong(nats_JSON *json, const char *fieldName, int64_t **array, int *arraySize)
{
    JSON_GET_ARRAY(TYPE_NUM, nats_JSONArrayAsLongs);
}

natsStatus
nats_JSONArrayAsULongs(nats_JSONArray *arr, uint64_t **array, int *arraySize)
{
    JSON_ARRAY_AS_NUM(uint64_t);
}

natsStatus
nats_JSONGetArrayULong(nats_JSON *json, const char *fieldName, uint64_t **array, int *arraySize)
{
    JSON_GET_ARRAY(TYPE_NUM, nats_JSONArrayAsULongs);
}

natsStatus
nats_JSONArrayAsObjects(nats_JSONArray *arr, nats_JSON ***array, int *arraySize)
{
    JSON_ARRAY_AS(nats_JSON*);
}

natsStatus
nats_JSONGetArrayObject(nats_JSON *json, const char *fieldName, nats_JSON ***array, int *arraySize)
{
    JSON_GET_ARRAY(TYPE_OBJECT, nats_JSONArrayAsObjects);
}

natsStatus
nats_JSONArrayAsArrays(nats_JSONArray *arr, nats_JSONArray ***array, int *arraySize)
{
    JSON_ARRAY_AS(nats_JSONArray*);
}

natsStatus
nats_JSONGetArrayArray(nats_JSON *json, const char *fieldName, nats_JSONArray ***array, int *arraySize)
{
    JSON_GET_ARRAY(TYPE_ARRAY, nats_JSONArrayAsArrays);
}

void
nats_JSONDestroy(nats_JSON *json)
{
    natsStrHashIter iter;
    void            *field = NULL;

    if (json == NULL)
        return;

    natsStrHashIter_Init(&iter, json->fields);
    while (natsStrHashIter_Next(&iter, NULL, &field))
    {
        natsStrHashIter_RemoveCurrent(&iter);
        _jsonFreeField((nats_JSONField*) field);
    }
    natsStrHash_Destroy(json->fields);
    NATS_FREE(json->str);
    NATS_FREE(json);
}

natsStatus
nats_EncodeTimeUTC(char *buf, size_t bufLen, int64_t timeUTC)
{
    int64_t     t  = timeUTC / (int64_t) 1E9;
    int64_t     ns = timeUTC - ((int64_t) t * (int64_t) 1E9);
    struct tm   tp;
    int         n;

    // We will encode at most: "YYYY:MM:DDTHH:MM:SS.123456789+12:34"
    // so we need at least 35+1 characters.
    if (bufLen < 36)
        return nats_setError(NATS_INVALID_ARG,
                             "buffer to encode UTC time is too small (%d), needs 36",
                             (int) bufLen);

    if (timeUTC == 0)
    {
        snprintf(buf, bufLen, "%s", "0001-01-01T00:00:00Z");
        return NATS_OK;
    }

    memset(&tp, 0, sizeof(struct tm));
#ifdef _WIN32
    _gmtime64_s(&tp, (const __time64_t*) &t);
#else
    gmtime_r((const time_t*) &t, &tp);
#endif
    n = (int) strftime(buf, bufLen, "%FT%T", &tp);
    if (n == 0)
        return nats_setDefaultError(NATS_ERR);

    if (ns > 0)
    {
        char nsBuf[15];
        int i, nd;

        nd = snprintf(nsBuf, sizeof(nsBuf), ".%" PRId64, ns);
        for (; (nd > 0) && (nsBuf[nd-1] == '0'); )
            nd--;

        for (i=0; i<nd; i++)
            *(buf+n++) = nsBuf[i];
    }
    *(buf+n) = 'Z';
    *(buf+n+1) = '\0';

    return NATS_OK;
}

void
nats_Base32_Init(void)
{
    const char  *alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    int         alphaLen  = (int) strlen(alphabet);
    int         i;

    for (i=0; i<(int)sizeof(base32DecodeMap); i++)
        base32DecodeMap[i] = (char) 0xFF;

    for (i=0; i<alphaLen; i++)
        base32DecodeMap[(int)alphabet[i]] = (char) i;
}

natsStatus
nats_Base32_DecodeString(const char *src, char *dst, int dstMax, int *dstLen)
{
    char        *ptr      = (char*) src;
    int         n         = 0;
    bool        done      = false;
    int         srcLen    = (int) strlen(src);
    int         remaining = srcLen;

    *dstLen = 0;

    while (remaining > 0)
    {
        char dbuf[8];
        int  dLen = 8;
        int  j;
        int  needs;

        for (j=0; j<8; )
        {
            int in;

            if (remaining == 0)
            {
                dLen = j;
                done  = true;
                break;
            }

            in = (int) *ptr;
            ptr++;
            remaining--;

            dbuf[j] = base32DecodeMap[in];
            // If invalid character, report the position but as the number of character
            // since beginning, not array index.
            if (dbuf[j] == (char) 0xFF)
                return nats_setError(NATS_ERR, "base32: invalid data at location %d", srcLen - remaining);
            j++;
        }

        needs = 0;
        switch (dLen)
        {
            case 8: needs = 5; break;
            case 7: needs = 4; break;
            case 5: needs = 3; break;
            case 4: needs = 2; break;
            case 2: needs = 1; break;
        }
        if (n+needs > dstMax)
            return nats_setError(NATS_INSUFFICIENT_BUFFER, "based32: needs %d bytes, max is %d", n+needs, dstMax);

        if (dLen == 8)
            dst[4] = dbuf[6]<<5 | dbuf[7];
        if (dLen >= 7)
            dst[3] = dbuf[4]<<7 | dbuf[5]<<2 | dbuf[6]>>3;
        if (dLen >= 5)
            dst[2] = dbuf[3]<<4 | dbuf[4]>>1;
        if (dLen >= 4)
            dst[1] = dbuf[1]<<6 | dbuf[2]<<1 | dbuf[3]>>4;
        if (dLen >= 2)
            dst[0] = dbuf[0]<<3 | dbuf[1]>>2;

        n += needs;

        if (!done)
            dst += 5;
    }

    *dstLen = n;

    return NATS_OK;
}

natsStatus
nats_Base64RawURL_EncodeString(const unsigned char *src, int srcLen, char **pDest)
{
    char        *dst   = NULL;
    int         dstLen = 0;
    int         n;
    int         di = 0;
    int         si = 0;
    int         remain = 0;
    uint32_t    val = 0;

    *pDest = NULL;

    if ((src == NULL) || (src[0] == '\0'))
        return NATS_OK;

    n = srcLen;
    dstLen = (n * 8 + 5) / 6;
    dst = NATS_CALLOC(1, dstLen + 1);
    if (dst == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    n = ((srcLen / 3) * 3);
    for (si = 0; si < n; )
    {
        // Convert 3x 8bit source bytes into 4 bytes
        val = (uint32_t)(src[si+0])<<16 | (uint32_t)(src[si+1])<<8 | (uint32_t)(src[si+2]);

        dst[di+0] = base64EncodeURL[val >> 18 & 0x3F];
        dst[di+1] = base64EncodeURL[val >> 12 & 0x3F];
        dst[di+2] = base64EncodeURL[val >>  6 & 0x3F];
        dst[di+3] = base64EncodeURL[val       & 0x3F];

        si += 3;
        di += 4;
    }

    remain = srcLen - si;
    if (remain == 0)
    {
        *pDest = dst;
        return NATS_OK;
    }

    // Add the remaining small block
    val = (uint32_t)src[si+0] << 16;
    if (remain == 2)
        val |= (uint32_t)src[si+1] << 8;

    dst[di+0] = base64EncodeURL[val >> 18 & 0x3F];
    dst[di+1] = base64EncodeURL[val >> 12 & 0x3F];

    if (remain == 2)
        dst[di+2] = base64EncodeURL[val >> 6 & 0x3F];

    *pDest = dst;

    return NATS_OK;
}

// Returns the 2-byte crc for the data provided.
uint16_t
nats_CRC16_Compute(unsigned char *data, int len)
{
    uint16_t    crc = 0;
    int         i;

    for (i=0; i<len; i++)
        crc = ((crc << 8) & 0xFFFF) ^ crc16tab[((crc>>8)^(uint16_t)(data[i]))&0x00FF];

    return crc;
}

// Checks the calculated crc16 checksum for data against the expected.
bool
nats_CRC16_Validate(unsigned char *data, int len, uint16_t expected)
{
    uint16_t crc = nats_CRC16_Compute(data, len);
    return crc == expected;
}

natsStatus
nats_ReadFile(natsBuffer **buffer, int initBufSize, const char *fn)
{
    natsStatus  s;
    FILE        *f      = NULL;
    natsBuffer  *buf    = NULL;
    char        *ptr    = NULL;
    int         total   = 0;

    if ((initBufSize <= 0) || nats_IsStringEmpty(fn))
        return nats_setDefaultError(NATS_INVALID_ARG);

    f = fopen(fn, "r");
    if (f == NULL)
        return nats_setError(NATS_ERR, "error opening file '%s': %s", fn, strerror(errno));

    s = natsBuf_Create(&buf, initBufSize);
    if (s == NATS_OK)
        ptr = natsBuf_Data(buf);
    while (s == NATS_OK)
    {
        int r = (int) fread(ptr, 1, (size_t) natsBuf_Available(buf), f);
        if (r == 0)
            break;

        total += r;
        natsBuf_MoveTo(buf, total);
        if (natsBuf_Available(buf) == 0)
            s = natsBuf_Expand(buf, natsBuf_Capacity(buf)*2);
        if (s == NATS_OK)
            ptr = natsBuf_Data(buf) + total;
    }

    // Close file. If there was an error, do not report possible closing error
    // as the actual error
    if (s != NATS_OK)
        fclose(f);
    else if (fclose(f) != 0)
        s = nats_setError(NATS_ERR, "error closing file '%s': '%s", fn, strerror(errno));

    if (s == NATS_OK)
    {
        natsBuf_AppendByte(buf, '\0');
        *buffer = buf;
    }
    else if (buf != NULL)
    {
        memset(natsBuf_Data(buf), 0, natsBuf_Capacity(buf));
        natsBuf_Destroy(buf);
    }

    return NATS_UPDATE_ERR_STACK(s);
}

void
nats_FreeAddrInfo(struct addrinfo *res)
{
    // Calling freeaddrinfo(NULL) is undefined behaviour.
    if (res == NULL)
        return;

    freeaddrinfo(res);
}

bool
nats_HostIsIP(const char *host)
{
    struct addrinfo hint;
    struct addrinfo *res = NULL;
    bool            isIP = true;

    memset(&hint, '\0', sizeof hint);

    hint.ai_family = PF_UNSPEC;
    hint.ai_flags = AI_NUMERICHOST;

    if (getaddrinfo(host, NULL, &hint, &res) != 0)
        isIP = false;

    nats_FreeAddrInfo(res);

    return isIP;
}

static bool
_isLineAnHeader(const char *ptr)
{
    char    *last   = NULL;
    int     len     = 0;
    int     count   = 0;
    bool    done    = false;

    // We are looking for a header. Based on the Go client's regex,
    // the strict requirement is that it ends with at least 3 consecutive
    // `-` characters. It must also have 3 consecutive `-` before that.
    // So the minimum size would be 6.
    len = (int) strlen(ptr);
    if (len < 6)
        return false;

    // First make sure that we have at least 3 `-` at the end.
    last = (char*) (ptr + len - 1);

    while ((*last == '-') && (last != ptr))
    {
        count++;
        last--;
        if (count == 3)
            break;
    }
    if (count != 3)
        return false;

    // Now from that point and going backward, we consider
    // to have proper header if we find again 3 consecutive
    // dashes.
    count = 0;
    while (!done)
    {
        if (*last == '-')
        {
            // We have at least `---`, we are done.
            if (++count == 3)
                return true;
        }
        else
        {
            // Reset.. we need 3 consecutive dashes
            count = 0;
        }
        if (last == ptr)
            done = true;
        else
            last--;
    }
    // If we are here, it means we did not find `---`
    return false;
}

natsStatus
nats_GetJWTOrSeed(char **val, const char *content, int item)
{
    natsStatus  s       = NATS_OK;
    char        *pch    = NULL;
    char        *str    = NULL;
    char        *saved  = NULL;
    int         curItem = 0;
    int         orgLen  = 0;
    char        *nt     = NULL;

    // First, make a copy of the original content since
    // we are going to call strtok on it, which alters it.
    str = NATS_STRDUP(content);
    if (str == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    orgLen = (int) strlen(str);

    pch = nats_strtok(str, "\n", &nt);
    while (pch != NULL)
    {
        if (_isLineAnHeader(pch))
        {
            // We got the start of the section. Save the next line
            // as the possible returned value if the following line
            // is a header too.
            pch = nats_strtok(NULL, "\n", &nt);
            saved = pch;

            while (pch != NULL)
            {
                pch = nats_strtok(NULL, "\n", &nt);
                if (pch == NULL)
                    break;

                // We tolerate empty string(s).
                if (*pch == '\0')
                    continue;

                break;
            }
            if (pch == NULL)
                break;

            if (_isLineAnHeader(pch))
            {
                // Is this the item we were looking for?
                if (curItem == item)
                {
                    // Return a copy of the saved line
                    *val = NATS_STRDUP(saved);
                    if (*val == NULL)
                        s = nats_setDefaultError(NATS_NO_MEMORY);

                    break;
                }
                else if (++curItem > 1)
                {
                    break;
                }
            }
        }
        pch = nats_strtok(NULL, "\n", &nt);
    }

    memset(str, 0, orgLen);
    NATS_FREE(str);

    // Nothing was found, return NATS_NOT_FOUND but don't set the stack error.
    if ((s == NATS_OK) && (*val == NULL))
        return NATS_NOT_FOUND;

    return NATS_UPDATE_ERR_STACK(s);
}
