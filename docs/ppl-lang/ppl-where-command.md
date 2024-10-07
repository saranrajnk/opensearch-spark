## PPL where Command

### Description
The ``where`` command bool-expression to filter the search result. The ``where`` command only return the result when bool-expression evaluated to true.


### Syntax
`where <boolean-expression>`

* bool-expression: optional. any expression which could be evaluated to boolean value.

### Example 1: Filter result set with condition

The example show fetch all the document from accounts index with .

PPL query:

    os> source=accounts | where account_number=1 or gender="F" | fields account_number, gender;
    fetched rows / total rows = 2/2
    +------------------+----------+
    | account_number   | gender   |
    |------------------+----------|
    | 1                | M        |
    | 13               | F        |
    +------------------+----------+

### Additional Examples

#### **Filters With Logical Conditions**
```
- `source = table | where c = 'test' AND a = 1 | fields a,b,c`
- `source = table | where c != 'test' OR a > 1 | fields a,b,c | head 1`
- `source = table | where c = 'test' NOT a > 1 | fields a,b,c`
- `source = table | where a = 1 | fields a,b,c`
- `source = table | where a >= 1 | fields a,b,c`
- `source = table | where a < 1 | fields a,b,c`
- `source = table | where b != 'test' | fields a,b,c`
- `source = table | where c = 'test' | fields a,b,c | head 3`
- `source = table | where ispresent(b)`
- `source = table | where isnull(coalesce(a, b)) | fields a,b,c | head 3`
- `source = table | where isempty(a)`
- `source = table | where case(length(a) > 6, 'True' else 'False') = 'True'`

- `source = table | eval status_category =
      case(a >= 200 AND a < 300, 'Success',
      a >= 300 AND a < 400, 'Redirection',
      a >= 400 AND a < 500, 'Client Error',
      a >= 500, 'Server Error'
      else 'Incorrect HTTP status code')
      | where case(a >= 200 AND a < 300, 'Success',
      a >= 300 AND a < 400, 'Redirection',
      a >= 400 AND a < 500, 'Client Error',
      a >= 500, 'Server Error'
      else 'Incorrect HTTP status code'
      ) = 'Incorrect HTTP status code'

- `source = table
      | eval factor = case(a > 15, a - 14, isnull(b), a - 7, a < 3, a + 1 else 1)
      | where case(factor = 2, 'even', factor = 4, 'even', factor = 6, 'even', factor = 8, 'even' else 'odd') = 'even'
      |  stats count() by factor`
```