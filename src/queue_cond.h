#ifndef QUEUE_COND_H
#define QUEUE_COND_H

class queue_cond_expr_t;

class queue_cond_t {
public:  
  
  class value_t {
  public:
    enum {
      null_t,
      int_t,
      float_t
    };
  protected:
    int t;
    long long l;
    double f;
  public:
    int type() const { return t; }
    long long to_i() const { return l; }
    double to_f() const { return f; }
    bool is_true() const {
      switch (t) {
      case int_t:
	return l != 0;
      case float_t:
	return f != 0;
      default:
	break;
      }
      return false;
    }
    bool is_false() const {
      switch (t) {
      case int_t:
	return l == 0;
      case float_t:
	return f == 0;
      default:
	break;
      }
      return false;
    }
    static value_t null_value() { return value_t(); }
    static value_t int_value(long long l) { return value_t(l); }
    static value_t float_value(double f) { return value_t(f); }
    static int compare(const value_t &x, const value_t &y) {
      if (x.t == null_t) {
	return y.t == null_t ? 0 : 1;
      } else if (y.t == null_t) {
	return x.t == null_t ? 0 : -1;
      } else if (x.t == int_t && y.t == int_t) {
	if (x.l < y.l) {
	  return -1;
	} else if (x.l > y.l) {
	  return 1;
	}
	return 0;
      } else {
	if (x.f < y.f) {
	  return -1;
	} else if (x.f > y.f) {
	  return 1;
	}
	return 0;
      }
    }
  private:
    explicit value_t() : t(null_t) {}
    explicit value_t(long long _l) : t(int_t), l(_l), f(_l) {}
    explicit value_t(double _f) : t(float_t), l(_f), f(_f) {}
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
    int column_index;
    column_node_t(int c) : column_index(c) {}
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
      fill(nodes, nodes + N, static_cast<node_t*>(NULL));
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
      if (vx.type() == value_t::null_t) {
	return value_t::null_value();
      }
      return static_cast<const T*>(this)->uop(vx);
    }
  };
  struct neg_op : public unary_op<neg_op> {
    value_t uop(const value_t &x) const {
      if (x.type() == value_t::int_t) {
	return value_t::int_value(-x.to_i());
      } else {
	return value_t::float_value(-x.to_f());
      }
    }
  };
  struct not_op : public unary_op<not_op> {
    value_t uop(const value_t &x) const {
      return value_t::int_value(x.type() == value_t::int_t
				? ! x.to_i() : ! x.to_f());
    }
  };
  struct bitinv_op : public unary_op<bitinv_op> {
    value_t uop(const value_t &x) const {
      return value_t::int_value(~ x.to_i());
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
      return value_t::int_value(nodes[0]->get_value(ctx).type()
				== value_t::null_t);
    }
  };
  struct isnotnull_op : public pop_op<1> {
    virtual value_t get_value(const queue_cond_t *ctx) const {
      return value_t::int_value(nodes[0]->get_value(ctx).type()
				!= value_t::null_t);
    }
  };
  template<class T> struct binary_op : public pop_op<2> {
    virtual value_t get_value(const queue_cond_t *ctx) const {
      value_t vx = nodes[0]->get_value(ctx);
      value_t vy = nodes[1]->get_value(ctx);
      if (vx.type() == value_t::null_t || vy.type() == value_t::null_t) {
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
#define BOP(klass, op)							\
  struct klass : public binary_op<klass> {				\
    value_t bop(const value_t &x, const value_t &y) const {		\
      if (x.type() == value_t::int_t && y.type() == value_t::int_t) {	\
	return value_t::int_value(x.to_i() op y.to_i());		\
      } else {								\
	return value_t::float_value(x.to_f() op y.to_f());		\
      }									\
    }									\
  }
  BOP(add_op, +);
  BOP(sub_op, -);
  BOP(mul_op, *);
  struct intdiv_op : public binary_op<intdiv_op> {
    value_t bop(const value_t &x, const value_t &y) const {
      if (y.to_i() == 0) {
	return value_t::null_value;
      }
      return value_t::int_value(x.to_i() / y.to_i());
    }
  };
  BOP(mod_op, %);
#undef BOP
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
      } else if (vx.type() == value_t::null_t || vy.type() == value_t::null_t) {
	return value_t::null_value();
      }
      return value_t::int_value(0);
    }
  };
  struct and_op : public pop_op<2> {
    virtual value_t get_value(const queue_cond_t *ctx) const {
      value_t vx = nodes[0]->get_value(ctx);
      value_t vy = nodes[1]->get_value(ctx);
      if (vx.type() == value_t::null_t && vy.type() == value_t::null_t) {
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
      if (x.type() == value_t::int_t && y.type() == value_t::int_t) {
	return value_t::int_value(static_cast<long long>(pow(x.l, y.l)));
      } else {
	return value_t::float_value(pow(x.f, y.f));
      }
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
  void set_value(int column_index, const value_t& value) {
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
  value_t get_value(int column_index) const {
    return columns[column_index].second;
  }
};

#endif
