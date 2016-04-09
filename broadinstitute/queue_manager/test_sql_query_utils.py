import logging
import cmap.io.setup_logger as setup_logger
import unittest
import sql_query_utils as squ


logger = logging.getLogger(setup_logger.LOGGER_NAME)


class TestSqlQueryUtils(unittest.TestCase):
	def test_check_for_apostraphes(self):
		items = ['a', 'b']
		r = squ.check_for_apostraphes(items)
		assert False == r, r

		expected = "hello 'world'"
		items.append(expected)
		r = squ.check_for_apostraphes(items)
		assert len(r) == 1, len(r)
		logger.debug("r:  {}".format(r))
		r = r[0]
		assert 2 == r[0], r[0]
		assert expected == r[1], r[1]

	def test_build_in_clause(self):
		items = ["a", "b"]
		r = squ.build_in_clause(items)
		assert r
		assert "'a', 'b'" == r, r
		
		expected = "c'd"
		items.append(expected)
		with self.assertRaises(Exception) as context:
			squ.build_in_clause(items)
		assert context.exception
		logger.debug("context.exception:  {}".format(context.exception))
		assert expected in str(context.exception)



if __name__ == "__main__":
	setup_logger.setup(verbose=True)

	unittest.main()
