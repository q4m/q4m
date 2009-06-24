#include <iostream>
#include "cac_mutex.h"

using namespace std;

struct loc {
  int x_;
  int y_;
  loc() : x_(), y_() {}
  loc(int x, int y) : x_(x), y_(y) {}
};
ostream& operator<<(ostream& os, const loc& l);

ostream& operator<<(ostream& os, const loc& l)
{
  os << l.x_ << ',' << l.y_;
  return os;
}

int main(void)
{
  // create an int value that requires mutex lock upon access
  cac_mutex_t<loc> cac(NULL);
  
  *cac_mutex_t<loc>::lockref(cac) = loc(1, 2);
  
  {
    cac_mutex_t<loc>::lockref l(cac);
    cout << *l << endl;
    l->x_++;
    cout << *l << endl;
  }
  
  return 0;
}
