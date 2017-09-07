using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Cilnn
{
    public class NanomsgException : Exception
    {
        private int _errorCode;
        public NanomsgException(string customError, int errorCode)
            : base(CreateError(customError, errorCode))
        {
            _errorCode = errorCode;
        }

        public NanomsgException(string customError)
            : this(customError, Nn.Errno())
        {

        }

        public NanomsgException()
            : this(null, Nn.Errno())
        {
        }

        /// <summary>
        /// This managed cache of error messages tries to avoid a repetitive re-marshaling of error strings.
        /// </summary>
        static readonly Dictionary<int, string> _errorMessages = new Dictionary<int, string>();

        public static string ErrorCodeToMessage(int errorCode)
        {
            string errorMessage;
            lock (_errorMessages)
                if (!_errorMessages.TryGetValue(errorCode, out errorMessage))
                    errorMessage = _errorMessages[errorCode] = Marshal.PtrToStringAnsi(Interop.nn_strerror(errorCode));
            return errorMessage;
        }

        static string CreateError(string customError, int errorCode)
        {
            string errorMessage = ErrorCodeToMessage(errorCode);

            if (string.IsNullOrEmpty(customError))
                return errorMessage;
            return string.Concat(customError, ": ", errorMessage);
        }
    }
}
