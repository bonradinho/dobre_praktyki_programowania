import pytest
import re

def is_palindrome(text: str) -> bool:
    text = ''.join(text.lower().split())
    return text == text[::-1]

def fibonacci(n: int) -> int:
    if n < 0:
        raise ValueError("n must be non-negative")
    if n == 0:
        return 0
    elif n == 1:
        return 1
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b

def count_vowels(text: str) -> int:
    vowels = set('aeiouyąęó')
    return sum(1 for char in text.lower() if char in vowels)

def calculate_discount(price: float, discount: float) -> float:
    if not 0 <= discount <= 1:
        raise ValueError("Discount must be between 0 and 1")
    return price * (1 - discount)

def flatten_list(nested_list: list) -> list:
    result = []
    for item in nested_list:
        if isinstance(item, list):
            result.extend(flatten_list(item))
        else:
            result.append(item)
    return result

def word_frequencies(text: str) -> dict:
    words = re.findall(r'\b\w+\b', text.lower())
    freq = {}
    for word in words:
        freq[word] = freq.get(word, 0) + 1
    return freq

def is_prime(n: int) -> bool:
    if n < 2:
        return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True

# Testy jednostkowe
class TestIsPalindrome:
    @pytest.mark.parametrize("text, expected", [
        ("kajak", True),
        ("Kobyła ma mały bok", True),
        ("python", False),
        ("", True),
        ("A", True)
    ])
    def test_is_palindrome(self, text, expected):
        assert is_palindrome(text) == expected

class TestFibonacci:
    @pytest.mark.parametrize("n, expected", [
        (0, 0),
        (1, 1),
        (5, 5),
        (10, 55)
    ])
    def test_fibonacci_valid(self, n, expected):
        assert fibonacci(n) == expected

    def test_fibonacci_negative(self):
        with pytest.raises(ValueError):
            fibonacci(-1)

class TestCountVowels:
    @pytest.mark.parametrize("text, expected", [
        ("Python", 1),
        ("AEIOUY", 6),
        ("bcd", 0),
        ("", 0),
        ("Próba żółwia", 4)
    ])
    def test_count_vowels(self, text, expected):
        assert count_vowels(text) == expected

class TestCalculateDiscount:
    @pytest.mark.parametrize("price, discount, expected", [
        (100, 0.2, 80.0),
        (50, 0, 50.0),
        (200, 1, 0.0)
    ])
    def test_valid_discount(self, price, discount, expected):
        assert calculate_discount(price, discount) == expected

    @pytest.mark.parametrize("price, discount", [
        (100, -0.1),
        (100, 1.5)
    ])
    def test_invalid_discount(self, price, discount):
        with pytest.raises(ValueError):
            calculate_discount(price, discount)

class TestFlattenList:
    @pytest.mark.parametrize("nested_list, expected", [
        ([1, 2, 3], [1, 2, 3]),
        ([1, [2, 3], [4, [5]]], [1, 2, 3, 4, 5]),
        ([], []),
        ([[[1]]], [1]),
        ([1, [2, [3, [4]]]], [1, 2, 3, 4])
    ])
    def test_flatten_list(self, nested_list, expected):
        assert flatten_list(nested_list) == expected

class TestWordFrequencies:
    @pytest.mark.parametrize("text, expected", [
        ("To be or not to be", {"to": 2, "be": 2, "or": 1, "not": 1}),
        ("Hello, hello!", {"hello": 2}),
        ("", {}),
        ("Python Python python", {"python": 3}),
        ("Ala ma kota, a kot ma Ale.", {"ala": 1, "ma": 2, "kota": 1, "a": 1, "kot": 1, "ale": 1})
    ])
    def test_word_frequencies(self, text, expected):
        assert word_frequencies(text) == expected

class TestIsPrime:
    @pytest.mark.parametrize("n, expected", [
        (2, True),
        (3, True),
        (4, False),
        (0, False),
        (1, False),
        (5, True),
        (97, True)
    ])
    def test_is_prime(self, n, expected):
        assert is_prime(n) == expected
