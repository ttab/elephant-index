// Code generated by smithy-go-codegen DO NOT EDIT.

package s3

import (
	"context"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	s3cust "github.com/aws/aws-sdk-go-v2/service/s3/internal/customizations"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"time"
)

// This action initiates a multipart upload and returns an upload ID. This upload
// ID is used to associate all of the parts in the specific multipart upload. You
// specify this upload ID in each of your subsequent upload part requests (see
// UploadPart (https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html)
// ). You also include this upload ID in the final request to either complete or
// abort the multipart upload request. For more information about multipart
// uploads, see Multipart Upload Overview (https://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html)
// . If you have configured a lifecycle rule to abort incomplete multipart uploads,
// the upload must complete within the number of days specified in the bucket
// lifecycle configuration. Otherwise, the incomplete multipart upload becomes
// eligible for an abort action and Amazon S3 aborts the multipart upload. For more
// information, see Aborting Incomplete Multipart Uploads Using a Bucket Lifecycle
// Policy (https://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html#mpu-abort-incomplete-mpu-lifecycle-config)
// . For information about the permissions required to use the multipart upload
// API, see Multipart Upload and Permissions (https://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html)
// . For request signing, multipart upload is just a series of regular requests.
// You initiate a multipart upload, send one or more requests to upload parts, and
// then complete the multipart upload process. You sign each request individually.
// There is nothing special about signing multipart upload requests. For more
// information about signing, see Authenticating Requests (Amazon Web Services
// Signature Version 4) (https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html)
// . After you initiate a multipart upload and upload one or more parts, to stop
// being charged for storing the uploaded parts, you must either complete or abort
// the multipart upload. Amazon S3 frees up the space used to store the parts and
// stop charging you for storing them only after you either complete or abort a
// multipart upload. Server-side encryption is for data encryption at rest. Amazon
// S3 encrypts your data as it writes it to disks in its data centers and decrypts
// it when you access it. Amazon S3 automatically encrypts all new objects that are
// uploaded to an S3 bucket. When doing a multipart upload, if you don't specify
// encryption information in your request, the encryption setting of the uploaded
// parts is set to the default encryption configuration of the destination bucket.
// By default, all buckets have a base level of encryption configuration that uses
// server-side encryption with Amazon S3 managed keys (SSE-S3). If the destination
// bucket has a default encryption configuration that uses server-side encryption
// with an Key Management Service (KMS) key (SSE-KMS), or a customer-provided
// encryption key (SSE-C), Amazon S3 uses the corresponding KMS key, or a
// customer-provided key to encrypt the uploaded parts. When you perform a
// CreateMultipartUpload operation, if you want to use a different type of
// encryption setting for the uploaded parts, you can request that Amazon S3
// encrypts the object with a KMS key, an Amazon S3 managed key, or a
// customer-provided key. If the encryption setting in your request is different
// from the default encryption configuration of the destination bucket, the
// encryption setting in your request takes precedence. If you choose to provide
// your own encryption key, the request headers you provide in UploadPart (https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html)
// and UploadPartCopy (https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html)
// requests must match the headers you used in the request to initiate the upload
// by using CreateMultipartUpload . you can request that Amazon S3 save the
// uploaded parts encrypted with server-side encryption with an Amazon S3 managed
// key (SSE-S3), an Key Management Service (KMS) key (SSE-KMS), or a
// customer-provided encryption key (SSE-C). To perform a multipart upload with
// encryption by using an Amazon Web Services KMS key, the requester must have
// permission to the kms:Decrypt and kms:GenerateDataKey* actions on the key.
// These permissions are required because Amazon S3 must decrypt and read data from
// the encrypted file parts before it completes the multipart upload. For more
// information, see Multipart upload API and permissions (https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html#mpuAndPermissions)
// and Protecting data using server-side encryption with Amazon Web Services KMS (https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html)
// in the Amazon S3 User Guide. If your Identity and Access Management (IAM) user
// or role is in the same Amazon Web Services account as the KMS key, then you must
// have these permissions on the key policy. If your IAM user or role belongs to a
// different account than the key, then you must have the permissions on both the
// key policy and your IAM user or role. For more information, see Protecting Data
// Using Server-Side Encryption (https://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html)
// . Access Permissions When copying an object, you can optionally specify the
// accounts or groups that should be granted specific permissions on the new
// object. There are two ways to grant the permissions using the request headers:
//   - Specify a canned ACL with the x-amz-acl request header. For more
//     information, see Canned ACL (https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL)
//     .
//   - Specify access permissions explicitly with the x-amz-grant-read ,
//     x-amz-grant-read-acp , x-amz-grant-write-acp , and x-amz-grant-full-control
//     headers. These parameters map to the set of permissions that Amazon S3 supports
//     in an ACL. For more information, see Access Control List (ACL) Overview (https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html)
//     .
//
// You can use either a canned ACL or specify access permissions explicitly. You
// cannot do both. Server-Side- Encryption-Specific Request Headers Amazon S3
// encrypts data by using server-side encryption with an Amazon S3 managed key
// (SSE-S3) by default. Server-side encryption is for data encryption at rest.
// Amazon S3 encrypts your data as it writes it to disks in its data centers and
// decrypts it when you access it. You can request that Amazon S3 encrypts data at
// rest by using server-side encryption with other key options. The option you use
// depends on whether you want to use KMS keys (SSE-KMS) or provide your own
// encryption keys (SSE-C).
//   - Use KMS keys (SSE-KMS) that include the Amazon Web Services managed key (
//     aws/s3 ) and KMS customer managed keys stored in Key Management Service (KMS)
//     – If you want Amazon Web Services to manage the keys used to encrypt data,
//     specify the following headers in the request.
//   - x-amz-server-side-encryption
//   - x-amz-server-side-encryption-aws-kms-key-id
//   - x-amz-server-side-encryption-context If you specify
//     x-amz-server-side-encryption:aws:kms , but don't provide
//     x-amz-server-side-encryption-aws-kms-key-id , Amazon S3 uses the Amazon Web
//     Services managed key ( aws/s3 key) in KMS to protect the data. All GET and PUT
//     requests for an object protected by KMS fail if you don't make them by using
//     Secure Sockets Layer (SSL), Transport Layer Security (TLS), or Signature Version
//     4. For more information about server-side encryption with KMS keys (SSE-KMS),
//     see Protecting Data Using Server-Side Encryption with KMS keys (https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html)
//     .
//   - Use customer-provided encryption keys (SSE-C) – If you want to manage your
//     own encryption keys, provide all the following headers in the request.
//   - x-amz-server-side-encryption-customer-algorithm
//   - x-amz-server-side-encryption-customer-key
//   - x-amz-server-side-encryption-customer-key-MD5 For more information about
//     server-side encryption with customer-provided encryption keys (SSE-C), see
//     Protecting data using server-side encryption with customer-provided encryption
//     keys (SSE-C) (https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html)
//     .
//
// Access-Control-List (ACL)-Specific Request Headers You also can use the
// following access control–related headers with this operation. By default, all
// objects are private. Only the owner has full access control. When adding a new
// object, you can grant permissions to individual Amazon Web Services accounts or
// to predefined groups defined by Amazon S3. These permissions are then added to
// the access control list (ACL) on the object. For more information, see Using
// ACLs (https://docs.aws.amazon.com/AmazonS3/latest/dev/S3_ACLs_UsingACLs.html) .
// With this operation, you can grant access permissions using one of the following
// two methods:
//   - Specify a canned ACL ( x-amz-acl ) — Amazon S3 supports a set of predefined
//     ACLs, known as canned ACLs. Each canned ACL has a predefined set of grantees and
//     permissions. For more information, see Canned ACL (https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL)
//     .
//   - Specify access permissions explicitly — To explicitly grant access
//     permissions to specific Amazon Web Services accounts or groups, use the
//     following headers. Each header maps to specific permissions that Amazon S3
//     supports in an ACL. For more information, see Access Control List (ACL)
//     Overview (https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html) .
//     In the header, you specify a list of grantees who get the specific permission.
//     To grant permissions explicitly, use:
//   - x-amz-grant-read
//   - x-amz-grant-write
//   - x-amz-grant-read-acp
//   - x-amz-grant-write-acp
//   - x-amz-grant-full-control You specify each grantee as a type=value pair,
//     where the type is one of the following:
//   - id – if the value specified is the canonical user ID of an Amazon Web
//     Services account
//   - uri – if you are granting permissions to a predefined group
//   - emailAddress – if the value specified is the email address of an Amazon Web
//     Services account Using email addresses to specify a grantee is only supported in
//     the following Amazon Web Services Regions:
//   - US East (N. Virginia)
//   - US West (N. California)
//   - US West (Oregon)
//   - Asia Pacific (Singapore)
//   - Asia Pacific (Sydney)
//   - Asia Pacific (Tokyo)
//   - Europe (Ireland)
//   - South America (São Paulo) For a list of all the Amazon S3 supported Regions
//     and endpoints, see Regions and Endpoints (https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region)
//     in the Amazon Web Services General Reference. For example, the following
//     x-amz-grant-read header grants the Amazon Web Services accounts identified by
//     account IDs permissions to read object data and its metadata:
//     x-amz-grant-read: id="11112222333", id="444455556666"
//
// The following operations are related to CreateMultipartUpload :
//   - UploadPart (https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html)
//   - CompleteMultipartUpload (https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
//   - AbortMultipartUpload (https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html)
//   - ListParts (https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html)
//   - ListMultipartUploads (https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html)
func (c *Client) CreateMultipartUpload(ctx context.Context, params *CreateMultipartUploadInput, optFns ...func(*Options)) (*CreateMultipartUploadOutput, error) {
	if params == nil {
		params = &CreateMultipartUploadInput{}
	}

	result, metadata, err := c.invokeOperation(ctx, "CreateMultipartUpload", params, optFns, c.addOperationCreateMultipartUploadMiddlewares)
	if err != nil {
		return nil, err
	}

	out := result.(*CreateMultipartUploadOutput)
	out.ResultMetadata = metadata
	return out, nil
}

type CreateMultipartUploadInput struct {

	// The name of the bucket to which to initiate the upload When using this action
	// with an access point, you must direct requests to the access point hostname. The
	// access point hostname takes the form
	// AccessPointName-AccountId.s3-accesspoint.Region.amazonaws.com. When using this
	// action with an access point through the Amazon Web Services SDKs, you provide
	// the access point ARN in place of the bucket name. For more information about
	// access point ARNs, see Using access points (https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html)
	// in the Amazon S3 User Guide. When you use this action with Amazon S3 on
	// Outposts, you must direct requests to the S3 on Outposts hostname. The S3 on
	// Outposts hostname takes the form
	// AccessPointName-AccountId.outpostID.s3-outposts.Region.amazonaws.com . When you
	// use this action with S3 on Outposts through the Amazon Web Services SDKs, you
	// provide the Outposts access point ARN in place of the bucket name. For more
	// information about S3 on Outposts ARNs, see What is S3 on Outposts (https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html)
	// in the Amazon S3 User Guide.
	//
	// This member is required.
	Bucket *string

	// Object key for which the multipart upload is to be initiated.
	//
	// This member is required.
	Key *string

	// The canned ACL to apply to the object. This action is not supported by Amazon
	// S3 on Outposts.
	ACL types.ObjectCannedACL

	// Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption
	// with server-side encryption using AWS KMS (SSE-KMS). Setting this header to true
	// causes Amazon S3 to use an S3 Bucket Key for object encryption with SSE-KMS.
	// Specifying this header with an object action doesn’t affect bucket-level
	// settings for S3 Bucket Key.
	BucketKeyEnabled bool

	// Specifies caching behavior along the request/reply chain.
	CacheControl *string

	// Indicates the algorithm you want Amazon S3 to use to create the checksum for
	// the object. For more information, see Checking object integrity (https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html)
	// in the Amazon S3 User Guide.
	ChecksumAlgorithm types.ChecksumAlgorithm

	// Specifies presentational information for the object.
	ContentDisposition *string

	// Specifies what content encodings have been applied to the object and thus what
	// decoding mechanisms must be applied to obtain the media-type referenced by the
	// Content-Type header field.
	ContentEncoding *string

	// The language the content is in.
	ContentLanguage *string

	// A standard MIME type describing the format of the object data.
	ContentType *string

	// The account ID of the expected bucket owner. If the bucket is owned by a
	// different account, the request fails with the HTTP status code 403 Forbidden
	// (access denied).
	ExpectedBucketOwner *string

	// The date and time at which the object is no longer cacheable.
	Expires *time.Time

	// Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object. This
	// action is not supported by Amazon S3 on Outposts.
	GrantFullControl *string

	// Allows grantee to read the object data and its metadata. This action is not
	// supported by Amazon S3 on Outposts.
	GrantRead *string

	// Allows grantee to read the object ACL. This action is not supported by Amazon
	// S3 on Outposts.
	GrantReadACP *string

	// Allows grantee to write the ACL for the applicable object. This action is not
	// supported by Amazon S3 on Outposts.
	GrantWriteACP *string

	// A map of metadata to store with the object in S3.
	Metadata map[string]string

	// Specifies whether you want to apply a legal hold to the uploaded object.
	ObjectLockLegalHoldStatus types.ObjectLockLegalHoldStatus

	// Specifies the Object Lock mode that you want to apply to the uploaded object.
	ObjectLockMode types.ObjectLockMode

	// Specifies the date and time when you want the Object Lock to expire.
	ObjectLockRetainUntilDate *time.Time

	// Confirms that the requester knows that they will be charged for the request.
	// Bucket owners need not specify this parameter in their requests. For information
	// about downloading objects from Requester Pays buckets, see Downloading Objects
	// in Requester Pays Buckets (https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectsinRequesterPaysBuckets.html)
	// in the Amazon S3 User Guide.
	RequestPayer types.RequestPayer

	// Specifies the algorithm to use to when encrypting the object (for example,
	// AES256).
	SSECustomerAlgorithm *string

	// Specifies the customer-provided encryption key for Amazon S3 to use in
	// encrypting data. This value is used to store the object and then it is
	// discarded; Amazon S3 does not store the encryption key. The key must be
	// appropriate for use with the algorithm specified in the
	// x-amz-server-side-encryption-customer-algorithm header.
	SSECustomerKey *string

	// Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
	// Amazon S3 uses this header for a message integrity check to ensure that the
	// encryption key was transmitted without error.
	SSECustomerKeyMD5 *string

	// Specifies the Amazon Web Services KMS Encryption Context to use for object
	// encryption. The value of this header is a base64-encoded UTF-8 string holding
	// JSON with the encryption context key-value pairs.
	SSEKMSEncryptionContext *string

	// Specifies the ID of the symmetric encryption customer managed key to use for
	// object encryption. All GET and PUT requests for an object protected by Amazon
	// Web Services KMS will fail if not made via SSL or using SigV4. For information
	// about configuring using any of the officially supported Amazon Web Services SDKs
	// and Amazon Web Services CLI, see Specifying the Signature Version in Request
	// Authentication (https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingAWSSDK.html#specify-signature-version)
	// in the Amazon S3 User Guide.
	SSEKMSKeyId *string

	// The server-side encryption algorithm used when storing this object in Amazon S3
	// (for example, AES256, aws:kms ).
	ServerSideEncryption types.ServerSideEncryption

	// By default, Amazon S3 uses the STANDARD Storage Class to store newly created
	// objects. The STANDARD storage class provides high durability and high
	// availability. Depending on performance needs, you can specify a different
	// Storage Class. Amazon S3 on Outposts only uses the OUTPOSTS Storage Class. For
	// more information, see Storage Classes (https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html)
	// in the Amazon S3 User Guide.
	StorageClass types.StorageClass

	// The tag-set for the object. The tag-set must be encoded as URL Query parameters.
	Tagging *string

	// If the bucket is configured as a website, redirects requests for this object to
	// another object in the same bucket or to an external URL. Amazon S3 stores the
	// value of this header in the object metadata.
	WebsiteRedirectLocation *string

	noSmithyDocumentSerde
}

type CreateMultipartUploadOutput struct {

	// If the bucket has a lifecycle rule configured with an action to abort
	// incomplete multipart uploads and the prefix in the lifecycle rule matches the
	// object name in the request, the response includes this header. The header
	// indicates when the initiated multipart upload becomes eligible for an abort
	// operation. For more information, see Aborting Incomplete Multipart Uploads
	// Using a Bucket Lifecycle Policy (https://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html#mpu-abort-incomplete-mpu-lifecycle-config)
	// . The response also includes the x-amz-abort-rule-id header that provides the
	// ID of the lifecycle configuration rule that defines this action.
	AbortDate *time.Time

	// This header is returned along with the x-amz-abort-date header. It identifies
	// the applicable lifecycle configuration rule that defines the action to abort
	// incomplete multipart uploads.
	AbortRuleId *string

	// The name of the bucket to which the multipart upload was initiated. Does not
	// return the access point ARN or access point alias if used. When using this
	// action with an access point, you must direct requests to the access point
	// hostname. The access point hostname takes the form
	// AccessPointName-AccountId.s3-accesspoint.Region.amazonaws.com. When using this
	// action with an access point through the Amazon Web Services SDKs, you provide
	// the access point ARN in place of the bucket name. For more information about
	// access point ARNs, see Using access points (https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html)
	// in the Amazon S3 User Guide. When you use this action with Amazon S3 on
	// Outposts, you must direct requests to the S3 on Outposts hostname. The S3 on
	// Outposts hostname takes the form
	// AccessPointName-AccountId.outpostID.s3-outposts.Region.amazonaws.com . When you
	// use this action with S3 on Outposts through the Amazon Web Services SDKs, you
	// provide the Outposts access point ARN in place of the bucket name. For more
	// information about S3 on Outposts ARNs, see What is S3 on Outposts (https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html)
	// in the Amazon S3 User Guide.
	Bucket *string

	// Indicates whether the multipart upload uses an S3 Bucket Key for server-side
	// encryption with Amazon Web Services KMS (SSE-KMS).
	BucketKeyEnabled bool

	// The algorithm that was used to create a checksum of the object.
	ChecksumAlgorithm types.ChecksumAlgorithm

	// Object key for which the multipart upload was initiated.
	Key *string

	// If present, indicates that the requester was successfully charged for the
	// request.
	RequestCharged types.RequestCharged

	// If server-side encryption with a customer-provided encryption key was
	// requested, the response will include this header confirming the encryption
	// algorithm used.
	SSECustomerAlgorithm *string

	// If server-side encryption with a customer-provided encryption key was
	// requested, the response will include this header to provide round-trip message
	// integrity verification of the customer-provided encryption key.
	SSECustomerKeyMD5 *string

	// If present, specifies the Amazon Web Services KMS Encryption Context to use for
	// object encryption. The value of this header is a base64-encoded UTF-8 string
	// holding JSON with the encryption context key-value pairs.
	SSEKMSEncryptionContext *string

	// If present, specifies the ID of the Amazon Web Services Key Management Service
	// (Amazon Web Services KMS) symmetric encryption customer managed key that was
	// used for the object.
	SSEKMSKeyId *string

	// The server-side encryption algorithm used when storing this object in Amazon S3
	// (for example, AES256, aws:kms ).
	ServerSideEncryption types.ServerSideEncryption

	// ID for the initiated multipart upload.
	UploadId *string

	// Metadata pertaining to the operation's result.
	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}

func (c *Client) addOperationCreateMultipartUploadMiddlewares(stack *middleware.Stack, options Options) (err error) {
	err = stack.Serialize.Add(&awsRestxml_serializeOpCreateMultipartUpload{}, middleware.After)
	if err != nil {
		return err
	}
	err = stack.Deserialize.Add(&awsRestxml_deserializeOpCreateMultipartUpload{}, middleware.After)
	if err != nil {
		return err
	}
	if err = addSetLoggerMiddleware(stack, options); err != nil {
		return err
	}
	if err = awsmiddleware.AddClientRequestIDMiddleware(stack); err != nil {
		return err
	}
	if err = smithyhttp.AddComputeContentLengthMiddleware(stack); err != nil {
		return err
	}
	if err = addResolveEndpointMiddleware(stack, options); err != nil {
		return err
	}
	if err = v4.AddComputePayloadSHA256Middleware(stack); err != nil {
		return err
	}
	if err = addRetryMiddlewares(stack, options); err != nil {
		return err
	}
	if err = addHTTPSignerV4Middleware(stack, options); err != nil {
		return err
	}
	if err = awsmiddleware.AddRawResponseToMetadata(stack); err != nil {
		return err
	}
	if err = awsmiddleware.AddRecordResponseTiming(stack); err != nil {
		return err
	}
	if err = addClientUserAgent(stack); err != nil {
		return err
	}
	if err = smithyhttp.AddErrorCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = smithyhttp.AddCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = swapWithCustomHTTPSignerMiddleware(stack, options); err != nil {
		return err
	}
	if err = addOpCreateMultipartUploadValidationMiddleware(stack); err != nil {
		return err
	}
	if err = stack.Initialize.Add(newServiceMetadataMiddleware_opCreateMultipartUpload(options.Region), middleware.Before); err != nil {
		return err
	}
	if err = addMetadataRetrieverMiddleware(stack); err != nil {
		return err
	}
	if err = addCreateMultipartUploadUpdateEndpoint(stack, options); err != nil {
		return err
	}
	if err = addResponseErrorMiddleware(stack); err != nil {
		return err
	}
	if err = v4.AddContentSHA256HeaderMiddleware(stack); err != nil {
		return err
	}
	if err = disableAcceptEncodingGzip(stack); err != nil {
		return err
	}
	if err = addRequestResponseLogging(stack, options); err != nil {
		return err
	}
	return nil
}

func newServiceMetadataMiddleware_opCreateMultipartUpload(region string) *awsmiddleware.RegisterServiceMetadata {
	return &awsmiddleware.RegisterServiceMetadata{
		Region:        region,
		ServiceID:     ServiceID,
		SigningName:   "s3",
		OperationName: "CreateMultipartUpload",
	}
}

// getCreateMultipartUploadBucketMember returns a pointer to string denoting a
// provided bucket member valueand a boolean indicating if the input has a modeled
// bucket name,
func getCreateMultipartUploadBucketMember(input interface{}) (*string, bool) {
	in := input.(*CreateMultipartUploadInput)
	if in.Bucket == nil {
		return nil, false
	}
	return in.Bucket, true
}
func addCreateMultipartUploadUpdateEndpoint(stack *middleware.Stack, options Options) error {
	return s3cust.UpdateEndpoint(stack, s3cust.UpdateEndpointOptions{
		Accessor: s3cust.UpdateEndpointParameterAccessor{
			GetBucketFromInput: getCreateMultipartUploadBucketMember,
		},
		UsePathStyle:                   options.UsePathStyle,
		UseAccelerate:                  options.UseAccelerate,
		SupportsAccelerate:             true,
		TargetS3ObjectLambda:           false,
		EndpointResolver:               options.EndpointResolver,
		EndpointResolverOptions:        options.EndpointOptions,
		UseARNRegion:                   options.UseARNRegion,
		DisableMultiRegionAccessPoints: options.DisableMultiRegionAccessPoints,
	})
}
