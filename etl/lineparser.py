import re

class LineParser:
    def __init__(self, tokens):
        self._re = self._build_regex(tokens)

    def parse(self, line):
        m = self._re.match(line.strip())
        if not m:
            raise ValueError('line does not match expected format')

        return [e if e != '' else None for e in m.groups()]

    def _build_regex(self, fmt):
        def _re_parts():
            for f in fmt:
                if f == 'q':
                    # quoted string or \N
                    yield r'(?:\\N|"([^"]*)")'
                elif f== 'nq':
                    # not quoted string or \N
                    yield r'(?:\\N|([^,]*))'
                else:
                    raise NotImplementedError()

        return re.compile(','.join(_re_parts()))
    #print(list(_fmt_tokens()))
