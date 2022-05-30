//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) : rows_(rows), cols_(cols) {
    size_t size = static_cast<size_t>(rows) * cols;
    linear_ = new T[size];
  }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual int GetRowCount() const = 0;

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int row, int col) const = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int row, int col, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  virtual ~Matrix() { delete[] linear_; }
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    data_ = new T *[rows];
    for (int i = 0; i < rows; ++i) {
      data_[i] = &(this->linear_[i * cols]);
    }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return this->rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return this->cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (row,col)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param row The row index
   * @param col The column index
   * @return The (row,col)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int row, int col) const override {
    if (row < 0 || row >= this->rows_ || col < 0 || col >= this->cols_) {
      LOG_DEBUG("row %d, rows_:%d", row, this->rows_);
      LOG_DEBUG("col %d, cols_:%d", col, this->cols_);
      throw Exception(ExceptionType::OUT_OF_RANGE, "RowMatrix::GetElement() out of range.");
    }
    return data_[row][col];
    //    throw NotImplementedException{"RowMatrix::GetElement() not implemented."};
  }

  /**
   * Set the (row,col)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param row The row index
   * @param col The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int row, int col, T val) override {
    if (row < 0 || row >= this->rows_ || col < 0 || col >= this->cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "RowMatrix::SetElement() out of range.");
    }
    data_[row][col] = val;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    if (this->rows_ < 0 || this->cols_ < 0 || static_cast<int>(source.size()) != this->rows_ * this->cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "RowMatrix::FillFrom() out of range.");
    }
    for (size_t i = 0; i < source.size(); ++i) {
      this->linear_[i] = source[i];
    }
    //    throw NotImplementedException{"RowMatrix::FillFrom() not implemented."};
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override {
    //    for (int i = 0; i < this->cols_; ++i) {
    //      delete [] data_[i];
    //    }
    delete[] data_;
  }

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    if (matrixA->GetColumnCount() != matrixB->GetColumnCount() || matrixA->GetRowCount() != matrixB->GetRowCount()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    RowMatrix<T> *matrix_res = new RowMatrix<T>(matrixA->GetRowCount(), matrixA->GetColumnCount());
    for (int i = 0; i < matrixA->GetRowCount(); ++i) {
      for (int j = 0; j < matrixA->GetColumnCount(); ++j) {
        T val = matrixA->GetElement(i, j) + matrixB->GetElement(i, j);
        matrix_res->SetElement(i, j, val);
      }
    }
    return std::unique_ptr<RowMatrix<T>>(matrix_res);
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    int rows1 = matrixA->GetRowCount();
    int cols1 = matrixA->GetColumnCount();
    int rows2 = matrixB->GetRowCount();
    int cols2 = matrixB->GetColumnCount();
    if (cols1 != rows2) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    RowMatrix<T> *matrix_res = new RowMatrix<T>(rows1, cols2);
    for (int row = 0; row < rows1; ++row) {
      for (int col = 0; col < cols2; ++col) {
        T val = 0;
        for (int k = 0; k < cols1; ++k) {
          val += matrixA->GetElement(row, k) * matrixB->GetElement(k, col);
        }
        matrix_res->SetElement(row, col, val);
      }
    }
    return std::unique_ptr<RowMatrix<T>>(matrix_res);
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    // TODO(P0): Add implementation
    auto res = Multiply(matrixA, matrixB);
    if (!res) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    res = Add(res.get(), matrixC);
    if (!res) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    return res;
  }
};
}  // namespace bustub
