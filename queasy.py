
import sqlparse


class QueasyDB():
    def __init__(self, queries_path)
        self.method_factory = QueasyMethodFactory(self, queries_path)


class QueasyMethod():
    def __init__(self, parent, sql, query_type, columns, params, replace):
        self.parent = parent
        self.sql = sql
        self.query_type = query_type
        self.params = params
        self.columns = columns
        self.replace = replace
    
    def __call__(self, *args, **kwargs):
        return self._exec(*args, **kwargs)
    
    def _exec(self, *args, **kwargs):
        params = {}
        if args and kwargs:
            raise Exception("Use only positional arguments or keyword arguments, not both")
        if args and len(args) == len(self.params):
            for i, v in enumerate(args):
                params.update({self.params[i]: v})
        if kwargs and len(kwargs) == len(self.params):
            params = kwargs
        if set(params) != set(self.params):
            raise Exception("The set of parameters does not match the set of parameter names")
        sql = self.sql
        if True in self.replace:
            for x, b in zip(params, self.replace):
                if not b: continue
                sql = sql.replace("{"+x+"}", params[x])
        match self.query_type:
            case "INSERT":
                self.parent.cursor.execute(sql, params)
                return self.parent.cursor.lastrowid
            case "SELECT":
                rows = self.parent.cursor.execute(sql, params).fetchall()
                if rows is None:
                    return []
                return [{c: row[i] for i, c in enumerate(self.columns)} for row in rows]
            case _:
                self.parent.cursor.execute(sql, params)
    
    def _first_column(self, dicts):
        if not dicts:
            return []
        return [d[self.columns[0]] for d in dicts]
    
    def _first_item(self, dicts):
        if not dicts:
            return None
        return dicts[0][self.columns[0]]
    
    def _first_row(self, dicts):
        if not dicts:
            return {}
        return dicts[0]
    
    def as_dict(self, *args, **kwargs):
        return self._first_row(self._exec(*args, **kwargs))
    
    def as_dicts(self, *args, **kwargs):
        return self._exec(*args, **kwargs)
    
    def as_item(self, *args, **kwargs):
        return self._first_item(self._exec(*args, **kwargs))
    
    def as_list(self, *args, **kwargs):
        return self._first_column(self._exec(*args, **kwargs))
    
    def as_tuple(self, *args, **kwargs):
        return tuple([x for x in self.as_dict(*args, **kwargs).values()])
    
    def as_tuples(self, *args, **kwargs):
        return [tuple([x for x in y.values()]) for y in self._exec(*args, **kwargs)]


class QueasyMethodFactory():
    def __init__(self, parent, queries_path):
        self.parent = parent
        sql_paths = list((Path(__file__).parent.absolute() / queries_path).glob("*.sql"))
        for sql_path in sql_paths:
            self.attach_query(sql_path)
    
    def attach_query(self, sql_path):
        name = Path(sql_path).stem
        sql = Path(sql_path).read_text()
        query = self.read_query(sql)
        setattr(self.parent, name, query)
    
    def read_query(self, sql):
        parsed = sqlparse.parse(sql)[0]
        query_type = parsed.get_type()
        # get params
        params = []
        replace = []
        _replace = False
        for token in parsed.flatten():
            # print("ttype", token.ttype, "token", token)
            if token.ttype==sqlparse.tokens.Name.Placeholder:
                params.append(str(token)[1:])
                replace.append(_replace)
            elif token.ttype==sqlparse.tokens.Keyword and _replace:
                params.append(str(token))
                replace.append(_replace)
            elif token.ttype==sqlparse.tokens.Error and str(token)=="{":
                _replace = True
            elif token.ttype==sqlparse.tokens.Error and str(token)=="}":
                _replace = False
        # get columns
        columns = []
        if query_type == "SELECT":
            capture = False
            _group = False
            _as = False
            for token in parsed.flatten():
                if token.ttype==sqlparse.tokens.Keyword.DML and str(token)=="SELECT":
                    capture = True
                elif token.ttype==sqlparse.tokens.Keyword and str(token)=="FROM":
                    break
                elif capture:
                    if token.ttype==sqlparse.tokens.Keyword and str(token)=="AS":
                        _as = True
                    elif token.ttype==sqlparse.tokens.Name:
                        if _group or _as:
                            columns.pop()
                            _group = False
                            _as = False
                        columns.append(str(token))
                        _group = True
                    elif token.ttype==sqlparse.tokens.Text.Whitespace:
                        _group = False
        return QueasyMethod(self.parent, sql, query_type, columns, params, replace)
