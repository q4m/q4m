#include <cassert>
#include <cstring>
#include <stack>
#include "boost/spirit/core.hpp"
#include "boost/spirit/symbols.hpp"
#include "boost/spirit/tree/ast.hpp"
#include "boost/spirit/utility/confix.hpp"
#include "boost/spirit/utility/escape_char.hpp"
#ifdef TEST
#include <string>
#include <iostream>
#endif
#include "queue_cond.h"

using namespace std;
using namespace boost::spirit;

#define OR_TOKEN "or"
#define XOR_TOKEN "xor"
#define AND_TOKEN "and"
#define NOT_TOKEN "not"
#define IS_TOKEN "is"
#define TRUE_TOKEN "true"
#define FALSE_TOKEN "false"
#define NULL_TOKEN "null"
#define DIV_TOKEN "div"
#define MOD_TOKEN "mod"
#define TOKEN_DELIMITER "\0"

#define RESERVED_TOKENS \
  OR_TOKEN TOKEN_DELIMITER \
  XOR_TOKEN TOKEN_DELIMITER \
  AND_TOKEN TOKEN_DELIMITER \
  NOT_TOKEN TOKEN_DELIMITER \
  IS_TOKEN TOKEN_DELIMITER \
  TRUE_TOKEN TOKEN_DELIMITER \
  FALSE_TOKEN TOKEN_DELIMITER \
  NULL_TOKEN TOKEN_DELIMITER \
  DIV_TOKEN TOKEN_DELIMITER \
  MOD_TOKEN TOKEN_DELIMITER

struct wait_expr_t : public grammar<wait_expr_t> {
  
  class parse_context {
    stack<queue_cond_t::node_t*> stk;
    const queue_cond_t *_cond;
    bool _is_error;
  public:
    parse_context(const queue_cond_t *c) : stk(), _cond(c), _is_error(false) {}
    ~parse_context() {
      while (! stk.empty()) {
	delete stk.top();
	stk.pop();
      }
    }
    void push(queue_cond_t::node_t *n) { stk.push(n); }
    queue_cond_t::node_t *pop() {
      queue_cond_t::node_t *n = stk.top();
      stk.pop();
      return n;
    }
    const queue_cond_t *cond() const { return _cond; }
    bool is_error() const { return _is_error; }
    void set_is_error() const {
      const_cast<parse_context*>(this)->_is_error = true;
    }
  private:
    parse_context(const parse_context&); // not defined
    parse_context& operator=(const parse_context&); // not defined
  };
  
  struct action_base {
    const parse_context *_ctx;
    action_base(const parse_context *c) : _ctx(c) {}
    parse_context *ctx() const { return const_cast<parse_context*>(_ctx); }
  };
  template <class Op> struct pop_action : action_base {
    pop_action(const parse_context *c) : action_base(c) {}
    void operator()(char const *, char const *) const {
      Op *n = new Op();
      for (size_t i = Op::pop_count; i != 0; i--) {
	n->nodes[i - 1] = ctx()->pop();
      }
      if (n->is_const()) {
	queue_cond_t::value_t c = n->get_value(NULL);
	delete n;
	ctx()->push(new queue_cond_t::const_node_t(c));
      } else {
	ctx()->push(n);
      }
    }
  };
  struct null_action : public action_base {
    null_action(const parse_context *c) : action_base(c) {}
    void operator()(char const *, char const *) const {
      queue_cond_t::value_t n = queue_cond_t::value_t::null_value();
      ctx()->push(new queue_cond_t::const_node_t(n));
    }
  };
  struct int_action : public action_base {
    int_action(const parse_context *c) : action_base(c) {}
    void operator()(long long l) const {
      queue_cond_t::value_t v = queue_cond_t::value_t::int_value(l);
      ctx()->push(new queue_cond_t::const_node_t(v));
    }
  };
  struct col_action : public action_base {
    col_action(const parse_context *c) : action_base(c) {}
    void operator()(int column_index) const {
      ctx()->push(new queue_cond_t::column_node_t(column_index));
    }
  };
  struct quoted_col_action : public action_base {
    quoted_col_action(const parse_context *c) : action_base(c) {}
    void operator()(const char *begin, const char *end) const {
      int column_index;
      if ((column_index = ctx()->cond()->find_column(begin + 1, end - 1))
	  == -1) {
	ctx()->set_is_error();
	queue_cond_t::value_t v = queue_cond_t::value_t::null_value();
	ctx()->push(new queue_cond_t::const_node_t(v));
	return;
      }
      ctx()->push(new queue_cond_t::column_node_t(column_index));
    }
  };
  
  template <typename S> struct definition {
    rule<S> or_expr, xor_expr, and_expr, not_expr, rel_expr, bitor_expr,
      bitand_expr, shift_expr, add_expr, mul_expr, bitxor_expr, unary_expr,
      str_node, col_node;
    symbols<> ident_node;
    definition(const wait_expr_t& s) {
      or_expr =
	xor_expr >>
	*(
	  ("||" >> xor_expr)[pop_action<queue_cond_t::or_op>(s.ctx)] |
	  (as_lower_d[OR_TOKEN] >> xor_expr)
	  [pop_action<queue_cond_t::or_op>(s.ctx)]
	  );
      xor_expr =
	and_expr >>
	*(
	  (as_lower_d[XOR_TOKEN] >> and_expr)
	  [pop_action<queue_cond_t::xor_op>(s.ctx)]
	  );
      and_expr =
	not_expr >>
	*(
	 ("&&" >> not_expr)[pop_action<queue_cond_t::and_op>(s.ctx)] |
	 (as_lower_d[AND_TOKEN] >> not_expr)
	 [pop_action<queue_cond_t::and_op>(s.ctx)]
	 );
      not_expr =
	(as_lower_d[NOT_TOKEN] >> rel_expr)
	[pop_action<queue_cond_t::not_op>(s.ctx)] |
	rel_expr;
      rel_expr =
	bitor_expr >>
	*(
	  ('=' >> bitor_expr)[pop_action<queue_cond_t::eq_op>(s.ctx)] |
	  ("!=" >> bitor_expr)[pop_action<queue_cond_t::ne_op>(s.ctx)] |
	  ("<=" >> bitor_expr)[pop_action<queue_cond_t::le_op>(s.ctx)] |
	  ('<' >> bitor_expr)[pop_action<queue_cond_t::lt_op>(s.ctx)] |
	  (">=" >> bitor_expr)[pop_action<queue_cond_t::ge_op>(s.ctx)] |
	  ('>' >> bitor_expr)[pop_action<queue_cond_t::gt_op>(s.ctx)] |
	  (
	   as_lower_d[IS_TOKEN] >>
	   (
	    as_lower_d[TRUE_TOKEN]
	    [pop_action<queue_cond_t::istrue_op>(s.ctx)] |
	    as_lower_d[FALSE_TOKEN]
	    [pop_action<queue_cond_t::isfalse_op>(s.ctx)] |
	    as_lower_d[NULL_TOKEN][pop_action<queue_cond_t::isnull_op>(s.ctx)] |
	    as_lower_d[NOT_TOKEN] >>
	    (
	     as_lower_d[TRUE_TOKEN]
	     [pop_action<queue_cond_t::isnottrue_op>(s.ctx)] |
	     as_lower_d[FALSE_TOKEN]
	     [pop_action<queue_cond_t::isnotfalse_op>(s.ctx)] |
	     as_lower_d[NULL_TOKEN]
	     [pop_action<queue_cond_t::isnotnull_op>(s.ctx)]
	     )
	    )
	   )
	  );
      bitor_expr =
	bitand_expr >>
	*(
	  ('|' >> bitand_expr)[pop_action<queue_cond_t::bitor_op>(s.ctx)]
	  );
      bitand_expr =
	shift_expr >>
	*(
	  ('&' >> shift_expr)[pop_action<queue_cond_t::bitand_op>(s.ctx)]
	  );
      shift_expr =
	add_expr >>
	*(
	  ("<<" >> add_expr)[pop_action<queue_cond_t::shl_op>(s.ctx)] |
	  (">>" >> add_expr)[pop_action<queue_cond_t::shr_op>(s.ctx)]
	  );
      add_expr =
	mul_expr >>
	*(
	  ('+' >> mul_expr)[pop_action<queue_cond_t::add_op>(s.ctx)] |
	  ('-' >> mul_expr)[pop_action<queue_cond_t::sub_op>(s.ctx)]
	  );
      mul_expr =
	bitxor_expr >>
	*(
	  ('*' >> bitxor_expr)[pop_action<queue_cond_t::mul_op>(s.ctx)] |
	  (DIV_TOKEN >> bitxor_expr)
	  [pop_action<queue_cond_t::intdiv_op>(s.ctx)] |
	  ('%' >> bitxor_expr)[pop_action<queue_cond_t::mod_op>(s.ctx)] |
	  (MOD_TOKEN >> bitxor_expr)[pop_action<queue_cond_t::mod_op>(s.ctx)]
	  );
      bitxor_expr =
	unary_expr >>
	*(
	  ('^' >> unary_expr)[pop_action<queue_cond_t::bitxor_op>(s.ctx)]
	  );
      unary_expr =
	("0x" >> uint_parser<long long,16,1,16>()[int_action(s.ctx)]) |
	('0' >> uint_parser<long long,8,1,22>()[int_action(s.ctx)]) |
	uint_parser<long long,10,1,19>()[int_action(s.ctx)] |
	('-' >> unary_expr)[pop_action<queue_cond_t::neg_op>(s.ctx)] |
	('~' >> unary_expr)[pop_action<queue_cond_t::bitinv_op>(s.ctx)] |
	('(' >> or_expr >> ')') |
	("pow(" >> or_expr >> ',' >> or_expr >> ')')
	[pop_action<queue_cond_t::pow_func>(s.ctx)] |
	as_lower_d[NULL_TOKEN][null_action(s.ctx)] |
#if 0
	str_node |
#endif
	col_node[quoted_col_action(s.ctx)] |
	as_lower_d[ident_node][col_action(s.ctx)];
#if 0
      str_node =
	lexeme_d[inner_node_d[
			      confix_p('"', *lex_escape_ch_p, '"') |
			      confix_p('\'', *lex_escape_ch_p, '\'')
			      ]];
#endif
      col_node = lexeme_d[confix_p('`', *lex_escape_ch_p, '`')];
      
      const vector<pair<char*, queue_cond_t::value_t> >& columns =
	s.ctx->cond()->get_columns();
      int idx = 0;
      for (vector<pair<char*, queue_cond_t::value_t> >::const_iterator i =
	     columns.begin();
	   i != columns.end();
	   ++i) {
	const char *reserved;
	for (reserved = RESERVED_TOKENS;
	     *reserved != '\0';
	     reserved += strlen(reserved) + 1) {
	  if (strcasecmp(i->first, reserved) == 0) {
	    break;
	  }
	}
	if (*reserved == '\0') {
	  ident_node.add(i->first, idx++);
	}
      }
    }
    const rule<S>& start() const { return or_expr; }
  };

  parse_context *ctx;
  
  wait_expr_t(parse_context *c) : ctx(c) {}
  
};

queue_cond_t::queue_cond_t()
: columns()
{
}

queue_cond_t::~queue_cond_t()
{
  for (std::vector<std::pair<char*, value_t> >::iterator i = columns.begin();
       i != columns.end();
       ++i) {
    delete [] i->first;
  }
}

void queue_cond_t::add_column(const char *name)
{
  char *n = new char [strlen(name) + 1];
  strcpy(n, name);
  columns.push_back(make_pair(n, value_t::null_value()));
}

queue_cond_t::node_t *queue_cond_t::compile_expression(const char *expr,
						       size_t len)
{
  wait_expr_t::parse_context ctx(this);
  if (parse(expr, expr + len, wait_expr_t(&ctx), space_p).full
      && ! ctx.is_error()) {
    return ctx.pop();
  }
  return NULL;
}

int queue_cond_t::find_column(const char *first, const char *last) const
{
  size_t len = last - first;
  int idx = 0;
  for (std::vector<std::pair<char*, value_t> >::const_iterator
	 i = columns.begin();
       i != columns.end();
       ++i) {
    if (strncasecmp(first, i->first, len) == 0) {
      return idx;
    }
    idx++;
  }
  return -1;
}

#ifdef TEST

int main(int argc, char **argv)
{
  string s;
  
  while (getline(cin, s)) {
    queue_cond_t cond;
    cond.add_column("a");
    cond.set_value(0, queue_cond_t::value_t::int_value(3));
    cond.add_column("not");
    cond.set_value(1, queue_cond_t::value_t::int_value(4));
    queue_cond_t::node_t *expr = cond.compile_expression(s.c_str(), s.size());
    if (expr != NULL) {
      cout << cond.evaluate(expr) << endl;
      delete expr;
    } else {
      cout << "error" << endl;
    }
  }
  
  return 0;
}

#endif
