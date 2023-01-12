package service

import (
	"context"
	"io"

	"github.com/gcesario203/fullcycle-go-grpc/internal/database"
	"github.com/gcesario203/fullcycle-go-grpc/internal/pb"
)

type CategoryService struct {
	pb.UnimplementedCategoryServiceServer
	CategoryDB database.Category
}

func NewCategoryService(categoryDB database.Category) *CategoryService {
	return &CategoryService{
		CategoryDB: categoryDB,
	}
}

func (c *CategoryService) CreateCategory(ctx context.Context, request *pb.CreateCategoryRequest) (*pb.Category, error) {
	category, err := c.CategoryDB.Create(request.Name, request.Description)

	if err != nil {
		return nil, err
	}

	categoryResponse := &pb.Category{
		Id:          category.ID,
		Name:        category.Name,
		Description: category.Description,
	}

	return categoryResponse, nil
}

func (c *CategoryService) ListCategories(ctx context.Context, request *pb.Blank) (*pb.CategoryList, error) {

	categories, err := c.CategoryDB.FindAll()

	if err != nil {
		return nil, err
	}

	var categoriesResponse []*pb.Category

	for _, category := range categories {
		categoryResponse := &pb.Category{
			Id:          category.ID,
			Name:        category.Name,
			Description: category.Description,
		}

		categoriesResponse = append(categoriesResponse, categoryResponse)
	}

	return &pb.CategoryList{Categories: categoriesResponse}, nil
}

func (c *CategoryService) FindCategoryById(ctx context.Context, request *pb.FindCategoryByIdRequest) (*pb.Category, error) {

	category, err := c.CategoryDB.Find(request.Id)

	if err != nil {
		return nil, err
	}

	categoryResponse := &pb.Category{
		Id:          category.ID,
		Name:        category.Name,
		Description: category.Description,
	}

	return categoryResponse, nil
}

func (c *CategoryService) CreateCategoryStream(stream pb.CategoryService_CreateCategoryStreamServer) error {
	categories := &pb.CategoryList{}

	for {
		category, err := stream.Recv()

		if err == io.EOF {
			return stream.SendAndClose(categories)
		}

		if err != nil {
			return err
		}

		categoryResult, err := c.CategoryDB.Create(category.Name, category.Description)

		if err != nil {
			return err
		}

		categories.Categories = append(categories.Categories, &pb.Category{
			Id:          categoryResult.ID,
			Name:        categoryResult.Name,
			Description: categoryResult.Description,
		})
	}
}
