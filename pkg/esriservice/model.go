package esriservice

type SpatialReferenceType struct {
	Wkid       int
	LatestWkid int
}

type ExtentType struct {
	XMin             float64
	YMin             float64
	XMax             float64
	YMax             float64
	SpatialReference SpatialReferenceType
}

type ServiceDetails struct {
	Extent        ExtentType `json:"extent"`
	InitialExtent ExtentType `json:"initialExtent"`
	FullExtent    ExtentType `json:"fullExtent"`
}

type RectType struct {
	Width  int
	Height int
}

type ExportImageInput struct {
	BoundingBox ExtentType
	Size        RectType
	ImageSR     int
	// Format is the format of the rendered image. One of jpgpng, png, png8, png24, png32, jpg, bmp, gif, tiff.
	Format string
	// PixelType is how to represent a pixel in the image data. One of C128, C64, F32, F64, S16, S32, S8, U1, U16, U2, U32, U4, U8.
	PixelType string
	// NoData is a list of values to treat as no data/transparent.
	NoData []int
}

type ExportImageOutput struct {
	Href   string
	Width  int
	Height int
	Extent ExtentType
	Scale  int
}
