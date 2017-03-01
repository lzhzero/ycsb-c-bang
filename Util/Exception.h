/*
 * Exception.h
 *
 *  Created on: Mar 1, 2017
 *      Author: neal
 */

#ifndef EXCEPTION_H_
#define EXCEPTION_H_
#include <exception>
namespace Ycsb {
namespace Util {

class Exception: public std::exception {
public:
	Exception(const std::string &message) :
			message_(message) {
	}

	~Exception() throw () {
	}
	const char* what() const throw () {
		return message_.c_str();
	}

private:
	std::string message_;
};

} // Util
} // Ycsb

#endif /* EXCEPTION_H_ */
