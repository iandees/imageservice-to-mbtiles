package esriservice

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

type EsriService struct {
	baseURL string
}

func (s *EsriService) GetDetails(ctx context.Context) (*ServiceDetails, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s?f=json", s.baseURL), nil)
	if err != nil {
		return nil, err
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	details := &ServiceDetails{}
	err = json.Unmarshal(data, details)
	if err != nil {
		return nil, err
	}

	return details, nil
}

func (s *EsriService) ExportImage(ctx context.Context, input *ExportImageInput) (*ExportImageOutput, error) {
	args := url.Values{}
	args.Set("f", "pjson")
	args.Set("bbox", fmt.Sprintf("%f,%f,%f,%f", input.BoundingBox.XMin, input.BoundingBox.YMin, input.BoundingBox.XMax, input.BoundingBox.YMax))
	args.Set("bboxSR", fmt.Sprintf("%d", input.BoundingBox.SpatialReference.Wkid))
	args.Set("size", fmt.Sprintf("%d,%d", input.Size.Width, input.Size.Height))
	args.Set("imageSR", fmt.Sprintf("%d", input.ImageSR))
	args.Set("format", input.Format)
	args.Set("pixelType", input.PixelType)

	if len(input.NoData) > 0 {
		stringNodata := make([]string, len(input.NoData))
		for i, nodata := range input.NoData {
			stringNodata[i] = fmt.Sprintf("%d", nodata)
		}

		args.Set("noData", strings.Join(stringNodata, ","))
	}

	url := fmt.Sprintf("%s/exportImage?%s", s.baseURL, args.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	details := &ExportImageOutput{}
	err = json.Unmarshal(data, details)
	if err != nil {
		return nil, err
	}

	return details, nil
}

func NewClient(baseURL string) *EsriService {
	return &EsriService{
		baseURL: baseURL,
	}
}
