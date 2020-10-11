#ifndef __LIST_H
#define __LIST_H

#include <mutex>
#include <type_traits>
#include <utility>
#include <cassert>

namespace bptdb {

struct ListTag {
    ListTag *prev{this};
    ListTag *next{this};
};

#define tag_declare(name, type, member) \
    constexpr static std::size_t name() {\
        return offsetof(type, member); \
    }\


template <typename T>
class List {
    static_assert(std::is_standard_layout<T>::value);
public:
    List(std::size_t off): m_off(off) {}
    void push_back(T *elem) {
        std::unique_lock lg(m_mtx);
        add(elem2tag(elem), m_head.prev, &m_head);
        m_size++;
    }
    void push_front(T *elem) {
        std::unique_lock lg(m_mtx);
        add(elem2tag(elem), &m_head, m_head.next);
        m_size++;
    }
    void erase(T *elem) {
        std::unique_lock lg(m_mtx);
        assert(m_size);
        erase(elem2tag(elem));
    }
    T *pop_front() {
        std::unique_lock lg(m_mtx);
        assert(m_size);
        ListTag *tag = m_head.next;
        erase(m_head.next);
        return tag2elem(tag);
    }
    T *pop_back() {
        std::unique_lock lg(m_mtx);
        assert(m_size);
        ListTag *tag = m_head.prev;
        erase(m_head.prev);
        return tag2elem(tag);
    }
    bool empty() {
        std::unique_lock lg(m_mtx);
        return m_size == 0;
    }
    std::size_t size() {
        std::unique_lock lg(m_mtx);
        return m_size;
    }
private:
    ListTag *elem2tag(T *elem) {
        return (ListTag *)((char *)elem + m_off);
    }
    T *tag2elem(ListTag *tag) {
        return (T *)((char *)tag - m_off);
    }
    void erase(ListTag *tag) {
        tag->prev->next = tag->next;
        tag->next->prev = tag->prev;
        m_size--;
    }
    void add(ListTag *newNode, ListTag *prevNode, ListTag *nextNode) {
        nextNode->prev = newNode;
        newNode->next  = nextNode;
        newNode->prev  = prevNode;
        prevNode->next = newNode;
    }
    ListTag m_head;
    std::size_t m_size{0};
    std::size_t m_off{0};
    std::mutex  m_mtx;
};

}// namespace bptdb
#endif
