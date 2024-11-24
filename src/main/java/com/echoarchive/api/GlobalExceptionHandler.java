package com.echoarchive.api;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.ServletRequestBindingException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.multipart.support.MissingServletRequestPartException;

import java.time.Instant;
import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<ApiError> notFound(NotFoundException e, HttpServletRequest req) {
        return build(HttpStatus.NOT_FOUND, e.getMessage(), req);
    }

    @ExceptionHandler({ConflictException.class, ObjectOptimisticLockingFailureException.class})
    public ResponseEntity<ApiError> conflict(RuntimeException e, HttpServletRequest req) {
        String msg = e instanceof ConflictException ? e.getMessage()
                : "Concurrent update to the same resource; retry the request";
        return build(HttpStatus.CONFLICT, msg, req);
    }

    @ExceptionHandler({BadRequestException.class, ConstraintViolationException.class,
            ServletRequestBindingException.class, MissingServletRequestPartException.class,
            MethodArgumentTypeMismatchException.class, HandlerMethodValidationException.class})
    public ResponseEntity<ApiError> badRequest(Exception e, HttpServletRequest req) {
        return build(HttpStatus.BAD_REQUEST, e.getMessage(), req);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> invalid(MethodArgumentNotValidException e, HttpServletRequest req) {
        String msg = e.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + ": " + f.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return build(HttpStatus.BAD_REQUEST, msg, req);
    }

    @ExceptionHandler({PayloadTooLargeException.class, MaxUploadSizeExceededException.class})
    public ResponseEntity<ApiError> tooLarge(Exception e, HttpServletRequest req) {
        return build(HttpStatus.PAYLOAD_TOO_LARGE, e.getMessage(), req);
    }

    @ExceptionHandler(IngestUnavailableException.class)
    public ResponseEntity<ApiError> unavailable(IngestUnavailableException e, HttpServletRequest req) {
        log.error("Ingest unavailable for {}", req.getRequestURI(), e);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("Retry-After", "5")
                .body(error(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage(), req));
    }

    private ResponseEntity<ApiError> build(HttpStatus status, String message, HttpServletRequest req) {
        return ResponseEntity.status(status).body(error(status, message, req));
    }

    private ApiError error(HttpStatus status, String message, HttpServletRequest req) {
        return new ApiError(status.value(), status.getReasonPhrase(), message, req.getRequestURI(),
                Instant.now());
    }
}
