/* Copyright 2009 Cybozu Labs, Inc. All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY CYBOZU LABS, INC. ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL CYBOZU LABS, INC. OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * 
 * The views and conclusions contained in the software and documentation are
 * those of the authors and should not be interpreted as representing official
 * policies, either expressed or implied, of Cybozu Labs, Inc.
 */

#ifndef cac_rwlock_h
#define cac_rwlock_h

template <typename T> class cac_rwlock_t {
public:
  
  class readref {
  protected:
    cac_rwlock_t<T>* m_;
  public:
    readref(cac_rwlock_t<T>& m) : m_(&m) {
      pthread_rwlock_rdlock(&m_->rwlock_);
    }
    ~readref() {
      pthread_rwlock_unlock(&m_->rwlock_);
    }
    operator const T*() { return &m_->t_; }
    const T& operator*() { return *operator const T*(); }
    const T* operator->() { return operator const T*(); }
  private:
    readref(const readref&);
    readref& operator=(const readref&);
    
    friend class cac_rwlock_t<T>::writeref;
    readref(cac_rwlock_t<T>& m, int) : m_(&m) {}
  };
  
  class writeref : public readref {
  public:
    writeref(cac_rwlock_t<T>& m) : readref(m, 0) {
      pthread_rwlock_wrlock(&m.rwlock_);
    }
    operator T*() {
      // GCC 4.1 seems to require the use of "this->"
      return &this->m_->t_;
    }
    T& operator*() { return *operator T*(); }
    T* operator->() { return operator T*(); }
  };
  
protected:
  friend class cac_rwlock_t<T>::readref;
  friend class cac_rwlock_t<T>::writeref;
  T t_;
  pthread_rwlock_t rwlock_;
public:
  cac_rwlock_t(pthread_rwlockattr_t* attr) : t_() {
    pthread_rwlock_init(&rwlock_, attr);
  }
  ~cac_rwlock_t() {
    pthread_rwlock_destroy(&rwlock_);
  }
  const T* unsafe_ref() const { return &t_; }
  T* unsafe_ref() { return &t_; }
  pthread_rwlock_t* rwlock() { return &rwlock_; }
private:
  cac_rwlock_t(const cac_rwlock_t&);
  cac_rwlock_t& operator=(const cac_rwlock_t&);
};

#endif
