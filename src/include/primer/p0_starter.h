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

#include <memory>

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  // TODO(P0): Add implementation
  Matrix(int r, int c) {
    rows = r;
    cols = c;
    linear = reinterpret_cast<T *>(calloc(r * c, sizeof(T)));
  }

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // TODO(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  // TODO(P0): Add implementation
  virtual ~Matrix() { free(linear); }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  // TODO(P0): Add implementation
  RowMatrix(int r, int c) : Matrix<T>(r, c) {
    data_ = reinterpret_cast<T **>(malloc(r * sizeof(T *)));
    for (int i = 0; i < r; ++i) {
      data_[i] = Matrix<T>::linear + i * c;
    }
  }

  // TODO(P0): Add implementation
  int GetRows() override { return Matrix<T>::rows; }

  // TODO(P0): Add implementation
  int GetColumns() override { return Matrix<T>::cols; }

  // TODO(P0): Add implementation
  T GetElem(int i, int j) override { return data_[i][j]; }

  // TODO(P0): Add implementation
  void SetElem(int i, int j, T val) override { data_[i][j] = val; }

  // TODO(P0): Add implementation
  void MatImport(T *arr) override {
    for (int i = 0; i < Matrix<T>::rows * Matrix<T>::cols; ++i) {
      Matrix<T>::linear[i] = arr[i];
    }
  }

  // TODO(P0): Add implementation
  ~RowMatrix() override {
    free(data_);
  }

 private:
  // 2D array containing the elements of the matrix in row-major format
  // TODO(P0): Allocate the array of row pointers in the constructor. Use these pointers
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    if (mat1->GetRows() == mat2->GetRows() && mat1->GetColumns() == mat2->GetColumns()) {
      int rows = mat1->GetRows();
      int columns = mat1->GetColumns();
      std::unique_ptr<RowMatrix<T>> uptr(new RowMatrix<T>(rows, columns));

      for (int i = 0; i < rows; ++i) {
        for (int j = 0; j < columns; ++j) {
          uptr->SetElem(i, j, mat1->GetElem(i, j) + mat2->GetElem(i, j));
        }
      }

      return uptr;
    }

    return std::unique_ptr<RowMatrix<T>>(nullptr);
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    if (mat1->GetColumns() != mat2->GetRows()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    int rows = mat1->GetRows();
    int columns = mat2->GetColumns();
    int limit = mat1->GetColumns();
    std::unique_ptr<RowMatrix<T>> uptr(new RowMatrix<T>(rows, columns));

    for (int i = 0; i < rows; ++i) {
      for (int j = 0; j < columns; ++j) {
        int sum = 0;

        for (int k = 0; k < limit; ++k) {
          sum += mat1->GetElem(i, k) * mat2->GetElem(k, j);
        }

        uptr->SetElem(i, j, sum);
      }
    }

    return uptr;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    // TODO(P0): Add code
    std::unique_ptr<RowMatrix<T>> uptr_1 = MultiplyMatrices(matA, matB);

    if (uptr_1 == nullptr){
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    return AddMatrices(uptr_1, matC);
  }
};
}  // namespace bustub
