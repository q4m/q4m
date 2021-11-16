#ifndef QUEUE_COND_H
#define QUEUE_COND_H

#include <math.h>

class queue_cond_expr_t;

class queue_cond_t {
public:  
  
  struct value_t {
    enum {
      null_t,
      int_t
    } type;
    long long l;
    bool is_true() const {
      switch (type) {
      case int_t:
	return l != 0;
      default:
	break;
      }
      return false;
    }
    bool is_false() const {
      switch (type) {
      case int_t:
	return l == 0;
      default:
	break;
      }
      return false;
    }
    static value_t null_value() { return value_t(); }
    static value_t int_value(long long l) { return value_t(l); }
    static int compare(const value_t &x, const value_t &y) {
      if (x.l < y.l) {
	return -1;
      } else if (x.l > y.l) {
	return 1;
      }
      return 0;
    }
  private:
    explicit value_t() : type(null_t) {}
    explicit value_t(long long _l) : type(int_t), l(_l) {}
  };
  
  struct node_t {
    virtual ~node_t() {}
    virtual value_t get_value(const queue_cond_t *ctx) const = 0;
    virtual bool is_const() const = 0;
  };
  struct null_node_t : public node_t {
    virtual value_t get_value(const queue_cond_t *) const {
      return value_t::null_value();
    }
    virtual bool is_const() const { return true; }
  };
  struct const_node_t : public node_t {
    value_t v;
    const_node_t(const value_t &_v) : v(_v) {}
    virtual value_t get_value(const queue_cond_t *) const {
      return v;
    }
    virtual bool is_const() const { return true; }
  };
  struct column_node_t : public node_t {
    size_t column_index;
    column_node_t(size_t c) : column_index(c) {}
    virtual value_t get_value(const queue_cond_t *ctx) const {
      return ctx->get_value(column_index);
    }
    virtual bool is_const() const { return false; }
  };
  template<size_t N> struct pop_op : public node_t {
    enum {
      pop_count = N
    };
    node_t *nodes[N];
    pop_op() {
      std::fill(nodes, nodes + N, static_cast<node_t*>(NULL));
    }
    virtual ~pop_op() {
      for (size_t i = 0; i < N; i++) {
	delete nodes[i];
      }
    }
    virtual bool is_const() const {
      for (size_t i = 0; i < N; i++) {
	if (! nodes[i]->is_const()) {
	  return false;
	}
      }
      return true;
    }
  };
  template<class T> struct unary_op : public pop_op<1> {
    virtual value_t get_value(const queue_cond_t *ctx) const {
      value_t vx = nodes[0]->get_value(ctx);
      if (vx.type == value_t::null_t) {
	return value_t::null_value();
      }
      return static_cast<const T*>(this)->uop(vx);
    }
  };
  struct neg_op : public unary_op<neg_op> {
    value_t uop(const value_t &x) const {
      return value_t::int_value(-x.l);
    }
  };
  struct not_op : public unary_op<not_op> {
    value_t uop(const value_t &x) const {
      return value_t::int_value(! x.l);
    }
  };
  struct bitinv_op : public unary_op<bitinv_op> {
    value_t uop(const value_t &x) const {
      return value_t::int_value(~ x.l);
    }
  };
  struct istrue_op : public pop_op<1> {
    virtual value_t get_value(const queue_cond_t *ctx) const {
      return value_t::int_value(nodes[0]->get_value(ctx).is_true());
    }
  };
  struct isnottrue_op : public pop_op<1> {
    virtual value_t get_value(const queue_cond_t *ctx) const {
      return value_t::int_value(! nodes[0]->get_value(ctx).is_true());
    }
  };
  struct isfalse_op : public pop_op<1> {
    virtual value_t get_value(const queue_cond_t *ctx) const {
      return value_t::int_value(nodes[0]->get_value(ctx).is_false());
    }
  };
  struct isnotfalse_op : public pop_op<1> {
    virtual value_t get_value(const queue_cond_t *ctx) const {
      return value_t::int_value(! nodes[0]->get_value(ctx).is_false());
    }
  };
  struct isnull_op : public pop_op<1> {
    virtual value_t get_value(const queue_cond_t *ctx) const {
      return value_t::int_value(nodes[0]->get_value(ctx).type
				== value_t::null_t);
    }
  };
  struct isnotnull_op : public pop_op<1> {
    virtual value_t get_value(const queue_cond_t *ctx) const {
      return value_t::int_value(nodes[0]->get_value(ctx).type
				!= value_t::null_t);
    }
  };
  template<class T> struct binary_op : public pop_op<2> {
    virtual value_t get_value(const queue_cond_t *ctx) const {
      value_t vx = nodes[0]->get_value(ctx);
      value_t vy = nodes[1]->get_value(ctx);
      if (vx.type == value_t::null_t || vy.type == value_t::null_t) {
	return value_t::null_value();
      }
      return static_cast<const T*>(this)->bop(vx, vy);
    }
  };
  template<class T> struct cmp_op : public binary_op<T> {
    value_t bop(const value_t &x, const value_t &y) const {
      return value_t::int_value(T::cmp2b(value_t::compare(x, y)));
    }
  };
  struct eq_op : public cmp_op<eq_op> {
    static bool cmp2b(int r) { return r == 0; }
  };
  struct ne_op : public cmp_op<ne_op> {
    static bool cmp2b(int r) { return r != 0; }
  };
  struct lt_op : public cmp_op<lt_op> {
    static bool cmp2b(int r) { return r < 0; }
  };
  struct le_op : public cmp_op<le_op> {
    static bool cmp2b(int r) { return r <= 0; }
  };
  struct gt_op : public cmp_op<gt_op> {
    static bool cmp2b(int r) { return r > 0; }
  };
  struct ge_op : public cmp_op<ge_op> {
    static bool cmp2b(int r) { return r >= 0; }
  };
  struct add_op : public binary_op<add_op> {
    value_t bop(const value_t &x, const value_t &y) const {
      return value_t::int_value(x.l + y.l);
    }
  };
  struct sub_op : public binary_op<sub_op> {
    value_t bop(const value_t &x, const value_t &y) const {
      return value_t::int_value(x.l - y.l);
    }
  };
  struct mul_op : public binary_op<mul_op> {
    value_t bop(const value_t &x, const value_t &y) const {
      return value_t::int_value(x.l * y.l);
    }
  };
  struct intdiv_op : public binary_op<intdiv_op> {
    value_t bop(const value_t &x, const value_t &y) const {
      if (y.l == 0) {
	return value_t::null_value();
      }
      return value_t::int_value(x.l / y.l);
    }
  };
  struct mod_op : public binary_op<mod_op> {
    value_t bop(const value_t &x, const value_t &y) const {
      if (y.l == 0) {
	return value_t::null_value();
      }
      return value_t::int_value(x.l % y.l);
    }
  };
  struct bitor_op : public binary_op<bitor_op> {
    value_t bop(const value_t &x, const value_t &y) const {
      return value_t::int_value(x.l | y.l);
    }
  };
  struct bitand_op : public binary_op<bitand_op> {
    value_t bop(const value_t &x, const value_t &y) const {
      return value_t::int_value(x.l & y.l);
    }
  };
  struct bitxor_op : public binary_op<bitxor_op> {
    value_t bop(const value_t &x, const value_t &y) const {
      return value_t::int_value(x.l ^ y.l);
    }
  };
  struct shl_op : public binary_op<shl_op> {
    value_t bop(const value_t &x, const value_t &y) const {
      return value_t::int_value(x.l << y.l);
    }
  };
  struct shr_op : public binary_op<shr_op> {
    value_t bop(const value_t &x, const value_t &y) const {
      return value_t::int_value(x.l >> y.l);
    }
  };
  struct or_op : public pop_op<2> {
    virtual value_t get_value(const queue_cond_t *ctx) const {
      value_t vx = nodes[0]->get_value(ctx);
      if (vx.is_true()) {
	return value_t::int_value(1);
      }
      value_t vy = nodes[1]->get_value(ctx);
      if (vy.is_true()) {
	return value_t::int_value(1);
      } else if (vx.type == value_t::null_t || vy.type == value_t::null_t) {
	return value_t::null_value();
      }
      return value_t::int_value(0);
    }
  };
  struct and_op : public pop_op<2> {
    virtual value_t get_value(const queue_cond_t *ctx) const {
      value_t vx = nodes[0]->get_value(ctx);
      value_t vy = nodes[1]->get_value(ctx);
      if (vx.type == value_t::null_t && vy.type == value_t::null_t) {
	return value_t::null_value();
      }
      return value_t::int_value(vx.is_true() && vy.is_true());
    }
  };
  struct xor_op : public binary_op<xor_op> {
    value_t bop(const value_t &x, const value_t &y) const {
      return value_t::int_value((! x.l) ^ (! y.l));
    }
  };
  struct pow_func : public binary_op<pow_func> {
    value_t bop(const value_t &x, const value_t &y) const {
      return value_t::int_value(static_cast<long long>(powl(x.l, y.l)));
    }
  };

protected:  
  std::vector<std::pair<char*, value_t> > columns;
public:
  // init functions
  queue_cond_t();
  ~queue_cond_t();
  void add_column(const char *name);
  // setup functions
  node_t *compile_expression(const char *expr, size_t len);
  // per-row evaluation
  void set_value(size_t column_index, const value_t& value) {
    assert(column_index < columns.size());
    columns[column_index].second = value;
  }
  bool evaluate(const node_t *expr) const {
    return expr->get_value(this).is_true();
  }
  // accessors
  const std::vector<std::pair<char*, value_t> >& get_columns() const {
    return columns;
  }
  int find_column(const char *first, const char *last) const;
  value_t get_value(size_t column_index) const {
    assert(column_index < columns.size());
    return columns[column_index].second;
  }
};

#endif
